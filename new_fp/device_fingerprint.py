import pandas as pd
import numpy as np
import uuid
import time
from collections import defaultdict
import psutil

# Try to use numba for critical functions
try:
    from numba import jit

    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False


    def jit(*args, **kwargs):
        def decorator(func):
            return func

        return decorator


class SmartFingerprintProcessor:
    """
    Smart fingerprint processor that maintains complete matching capability while optimizing performance:
    1. Keep unique fingerprint signatures with their latest/representative values
    2. Incremental signature updates when matches are found
    3. Efficient signature-based matching instead of full index searches
    4. Memory-efficient storage of fingerprint metadata
    """

    def __init__(self, debug_anchor_value=None):
        # Store fingerprint signatures: fingerprint_id -> {feature -> latest_value}
        self.fingerprint_signatures = {}
        # Reverse lookup: feature_value -> set of fingerprint_ids that have this value
        self.feature_to_fingerprints = {}
        # Track which features are most discriminative for each fingerprint
        self.fingerprint_discriminators = {}

        self.debug_anchor_value = debug_anchor_value
        self.debug_logs = []  # Store all debug events

    def _debug_log(self, message, row_idx=None):
        """Log debug message if we're tracking this anchor."""
        log_entry = {
            'row_idx': row_idx,
            'message': message,
            'timestamp': time.time()
        }
        self.debug_logs.append(log_entry)
        print(f"[DEBUG Row {row_idx}] {message}")

    def process_fingerprints_smart(self, df, initial_anchor_feature, search_space_reducers,
                                   final_identification_features):
        """
        Smart processing that maintains complete fingerprint signatures.
        """
        n_rows = len(df)
        print(f"Processing {n_rows:,} rows with smart signature matching...")

        if self.debug_anchor_value:
            debug_rows = df[df[initial_anchor_feature] == self.debug_anchor_value].index.tolist()
            print(f"\n{'='*80}")
            print(f"DEBUG MODE: Tracking anchor value '{self.debug_anchor_value}'")
            print(f"Found {len(debug_rows)} rows with this anchor: {debug_rows[:10]}")
            print(f"{'='*80}\n")

        # Preprocess features
        encoded_arrays, feature_info = self._preprocess_features(
            df, initial_anchor_feature, search_space_reducers, final_identification_features
        )

        # DEBUG: Show encoding for our target anchor value
        if self.debug_anchor_value:
            anchor_series = df[initial_anchor_feature]
            mask = anchor_series == self.debug_anchor_value
            if mask.any():
                encoded_val = encoded_arrays[initial_anchor_feature][mask][0]
                print(f"\n{'=' * 80}")
                print(f"ENCODING INFO:")
                print(f"Original value: '{self.debug_anchor_value}'")
                print(f"Encoded as: {encoded_val}")
                print(f"Category index: {encoded_val}")

                # Show the category mapping
                categories = feature_info[initial_anchor_feature]['categories']
                print(f"Total categories: {len(categories)}")
                if encoded_val >= 0 and encoded_val < len(categories):
                    print(f"Decodes back to: '{categories[encoded_val]}'")
                print(f"{'=' * 80}\n")

        all_features = [initial_anchor_feature] + search_space_reducers + final_identification_features

        # Initialize feature lookup structures
        for feature in all_features:
            self.feature_to_fingerprints[feature] = defaultdict(set)

        # Pre-allocate results
        new_fingerprints = np.empty(n_rows, dtype=object)
        is_new_fingerprint = np.zeros(n_rows, dtype=bool)
        match_at_feature = np.full(n_rows, 'No Match', dtype=object)

        start_time = time.time()

        for current_idx in range(n_rows):
            if current_idx % 10000 == 0:
                self._print_progress(current_idx, n_rows, start_time)

            # Extract current row signature
            current_signature = self._extract_signature(current_idx, encoded_arrays, all_features)

            # DEBUG: Check if this is one of our target rows
            is_debug_row = False
            if self.debug_anchor_value:
                original_anchor_val = df.iloc[current_idx][initial_anchor_feature]
                if original_anchor_val == self.debug_anchor_value:
                    is_debug_row = True
                    self._debug_log(f"\n{'=' * 80}", current_idx)
                    self._debug_log(f"PROCESSING TARGET ROW", current_idx)
                    self._debug_log(f"Original anchor value: '{original_anchor_val}'", current_idx)
                    self._debug_log(f"Encoded anchor value: {current_signature.get(initial_anchor_feature)}",
                                    current_idx)
                    self._debug_log(f"Full signature: {current_signature}", current_idx)

                    # Show current state of feature_to_fingerprints for anchor
                    anchor_encoded = current_signature.get(initial_anchor_feature)
                    if anchor_encoded is not None:
                        existing_fps = self.feature_to_fingerprints[initial_anchor_feature].get(anchor_encoded, set())
                        self._debug_log(f"Existing fingerprints with this encoded anchor: {existing_fps}", current_idx)

                        # Show details of each existing fingerprint
                        for fp_id in existing_fps:
                            fp_sig = self.fingerprint_signatures.get(fp_id, {})
                            self._debug_log(f"  Fingerprint {fp_id}: {fp_sig}", current_idx)

            # Find matching fingerprint using smart signature matching
            match_result = self._find_match_by_signature(
                current_signature, initial_anchor_feature, search_space_reducers,
                final_identification_features, all_features
            )

            if match_result:
                fingerprint_id = match_result['fingerprint']
                new_fingerprints[current_idx] = fingerprint_id
                is_new_fingerprint[current_idx] = False
                match_at_feature[current_idx] = match_result['feature']

                # Update fingerprint signature with new values (incremental learning)
                self._update_fingerprint_signature(fingerprint_id, current_signature, all_features)

            else:
                # Create new fingerprint
                fingerprint_id = f"new_{uuid.uuid4()}"
                new_fingerprints[current_idx] = fingerprint_id
                is_new_fingerprint[current_idx] = True
                match_at_feature[current_idx] = 'No Match'

                # Register new fingerprint signature
                self._register_new_fingerprint(fingerprint_id, current_signature, all_features)

        # Create result DataFrame
        result_df = df.copy()
        result_df['new_fingerprint'] = new_fingerprints
        result_df['is_new_fingerprint'] = is_new_fingerprint
        result_df['match_at_feature'] = match_at_feature

        print(f"Final fingerprint count: {len(self.fingerprint_signatures):,}")
        return result_df

    def _preprocess_features(self, df, initial_anchor_feature, search_space_reducers,
                             final_identification_features):
        """Enhanced preprocessing with better categorical encoding."""
        all_features = [initial_anchor_feature] + search_space_reducers + final_identification_features
        encoded_arrays = {}
        feature_info = {}

        print("Preprocessing features...")
        for feature in all_features:
            # More efficient cleaning
            series = df[feature].astype('string')

            # Replace empty values more efficiently
            mask = series.isin(['[]', '{}', '', 'nan', 'None', np.nan]) | series.isna()
            series = series.where(~mask, None)  # Use None instead of np.nan

            # Use pandas categorical for better memory usage and faster comparison
            cat_series = series.astype('category')
            encoded_arrays[feature] = cat_series.cat.codes.values
            feature_info[feature] = {
                'categories': cat_series.cat.categories,
                'n_categories': len(cat_series.cat.categories)
            }


        return encoded_arrays, feature_info

    def _extract_signature(self, row_idx, encoded_arrays, all_features):
        """Extract signature for current row."""
        signature = {}
        for feature in all_features:
            code = encoded_arrays[feature][row_idx]
            signature[feature] = code if code >= 0 else None
        return signature

    def _find_match_by_signature(self, current_signature, initial_anchor_feature,
                                 search_space_reducers, final_identification_features, all_features,
                                 debug_row_idx=None):
        """
        Smart signature-based matching that's much faster than index intersection.
        """

        # Phase 1: Quick anchor check
        anchor_value = current_signature.get(initial_anchor_feature)

        if debug_row_idx is not None:
            self._debug_log(f"Phase 1: Anchor check", debug_row_idx)
            self._debug_log(f"  Looking for anchor_value (encoded): {anchor_value}", debug_row_idx)
            self._debug_log(
                f"  Available encoded anchor values in lookup: {list(self.feature_to_fingerprints[initial_anchor_feature].keys())[:20]}",
                debug_row_idx)

        if anchor_value is not None:
            anchor_candidates = self.feature_to_fingerprints[initial_anchor_feature].get(anchor_value, set())
            if anchor_candidates:
                # Check if any of these candidates is a perfect match on anchor
                for fingerprint_id in anchor_candidates:
                    if self._is_signature_match(fingerprint_id, current_signature, [initial_anchor_feature]):
                        return {'fingerprint': fingerprint_id, 'feature': initial_anchor_feature}

        # Phase 2: Search space reduction using signature intersection
        candidate_fingerprints = None

        # Find the most discriminative reducer feature first
        best_feature = None
        min_candidates = float('inf')

        for feature in search_space_reducers:
            feature_value = current_signature.get(feature)
            if feature_value is None:
                continue

            feature_candidates = self.feature_to_fingerprints[feature].get(feature_value, set())
            if len(feature_candidates) < min_candidates and len(feature_candidates) > 0:
                min_candidates = len(feature_candidates)
                best_feature = feature
                candidate_fingerprints = feature_candidates.copy()

        if candidate_fingerprints is None or not candidate_fingerprints:
            return None

        # Intersect with other reducer features
        for feature in search_space_reducers:
            if feature == best_feature:
                continue

            feature_value = current_signature.get(feature)
            if feature_value is None:
                continue

            feature_candidates = self.feature_to_fingerprints[feature].get(feature_value, set())
            candidate_fingerprints &= feature_candidates

            if not candidate_fingerprints:
                return None

        # Phase 3: Final identification using signature matching
        for feature in final_identification_features:
            feature_value = current_signature.get(feature)
            if feature_value is None:
                continue

            for fingerprint_id in candidate_fingerprints:
                if self._is_signature_match(fingerprint_id, current_signature, [feature]):
                    return {'fingerprint': fingerprint_id, 'feature': feature}

        return None

    def _is_signature_match(self, fingerprint_id, current_signature, features_to_check):
        """Check if current signature matches fingerprint signature on specified features."""
        fingerprint_sig = self.fingerprint_signatures.get(fingerprint_id, {})

        for feature in features_to_check:
            current_val = current_signature.get(feature)
            fingerprint_val = fingerprint_sig.get(feature)

            # Skip None/empty values
            if current_val is None or fingerprint_val is None:
                continue

            if current_val == fingerprint_val:
                return True

        return False

    # '''
    def _update_fingerprint_signature(self, fingerprint_id, current_signature, all_features):
        """
        Update fingerprint signature with new values (incremental learning).
        This allows the fingerprint to dynamically evolve with latest values.
        """
        if fingerprint_id not in self.fingerprint_signatures:
            self.fingerprint_signatures[fingerprint_id] = {}

        fingerprint_sig = self.fingerprint_signatures[fingerprint_id]
        updated = False

        for feature in all_features:
            current_val = current_signature.get(feature)
            if current_val is None:
                continue

            existing_val = fingerprint_sig.get(feature)

            # If this feature wasn't seen before, add it
            if existing_val is None:
                fingerprint_sig[feature] = current_val
                self.feature_to_fingerprints[feature][current_val].add(fingerprint_id)
                updated = True

            # If we see a different value for this feature, update to latest value
            elif existing_val != current_val:
                # Remove fingerprint from old value mapping
                if existing_val in self.feature_to_fingerprints[feature]:
                    self.feature_to_fingerprints[feature][existing_val].discard(fingerprint_id)
                    # # Clean up empty sets to save memory
                    # if not self.feature_to_fingerprints[feature][existing_val]:
                    #     del self.feature_to_fingerprints[feature][existing_val]

                # Update to new value
                fingerprint_sig[feature] = current_val
                self.feature_to_fingerprints[feature][current_val].add(fingerprint_id)
                updated = True

                # print(f"Updated fingerprint {fingerprint_id} feature '{feature}': {existing_val} → {current_val}")

        # Track discriminative features for this fingerprint
        if updated:
            self._update_discriminators(fingerprint_id)
    # '''

    '''
    def _update_fingerprint_signature(self, fingerprint_id, current_signature, all_features, debug_row_idx=None):
        """
        Update fingerprint signature with new values (incremental learning).
        This allows the fingerprint to dynamically evolve with latest values.
        """
        if fingerprint_id not in self.fingerprint_signatures:
            self.fingerprint_signatures[fingerprint_id] = {}

        fingerprint_sig = self.fingerprint_signatures[fingerprint_id]
        updated = False

        if debug_row_idx is not None:
            self._debug_log(f"Updating fingerprint signature for {fingerprint_id}", debug_row_idx)

        # Get the anchor feature (first in all_features list)
        anchor_feature = all_features[0]  # This is initial_anchor_feature

        for feature in all_features:
            current_val = current_signature.get(feature)
            if current_val is None:
                continue

            existing_val = fingerprint_sig.get(feature)

            # If this feature wasn't seen before, add it
            if existing_val is None:
                fingerprint_sig[feature] = current_val
                self.feature_to_fingerprints[feature][current_val].add(fingerprint_id)
                updated = True

                if debug_row_idx is not None:
                    self._debug_log(f"  Added new feature '{feature}': {current_val}", debug_row_idx)

            # If we see a different value for this feature, update to latest value
            # BUT NEVER UPDATE THE ANCHOR - it defines the fingerprint's identity
            elif existing_val != current_val and feature != anchor_feature:  # ← KEY FIX
                if debug_row_idx is not None:
                    self._debug_log(f"  ⚠️  Updating feature '{feature}': {existing_val} → {current_val}",
                                    debug_row_idx)

                # Remove fingerprint from old value mapping
                if existing_val in self.feature_to_fingerprints[feature]:
                    self.feature_to_fingerprints[feature][existing_val].discard(fingerprint_id)
                    # Clean up empty sets to save memory
                    if not self.feature_to_fingerprints[feature][existing_val]:
                        del self.feature_to_fingerprints[feature][existing_val]

                # Update to new value
                fingerprint_sig[feature] = current_val
                self.feature_to_fingerprints[feature][current_val].add(fingerprint_id)
                updated = True

            elif existing_val != current_val and feature == anchor_feature:
                # This should NEVER happen - log a warning
                if debug_row_idx is not None:
                    self._debug_log(f"  ⚠️⚠️⚠️  WARNING: Anchor feature value changed! {existing_val} → {current_val}",
                                    debug_row_idx)

        # Track discriminative features for this fingerprint
        if updated:
            self._update_discriminators(fingerprint_id)
    '''


    def _register_new_fingerprint(self, fingerprint_id, current_signature, all_features):
        """Register a new fingerprint with its signature."""
        self.fingerprint_signatures[fingerprint_id] = {}

        for feature in all_features:
            current_val = current_signature.get(feature)
            if current_val is not None:
                self.fingerprint_signatures[fingerprint_id][feature] = current_val
                self.feature_to_fingerprints[feature][current_val].add(fingerprint_id)

        self._update_discriminators(fingerprint_id)

    def _update_discriminators(self, fingerprint_id):
        """Track which features are most discriminative for this fingerprint."""
        if fingerprint_id not in self.fingerprint_discriminators:
            self.fingerprint_discriminators[fingerprint_id] = {}

        fingerprint_sig = self.fingerprint_signatures[fingerprint_id]

        for feature, value in fingerprint_sig.items():
            if value is not None:
                # Count how many other fingerprints share this feature value
                sharing_count = len(self.feature_to_fingerprints[feature][value]) - 1  # Exclude self
                self.fingerprint_discriminators[fingerprint_id][feature] = sharing_count

    def _print_progress(self, current_idx, n_rows, start_time):
        """Enhanced progress reporting with memory and fingerprint stats."""
        elapsed = time.time() - start_time
        rate = current_idx / elapsed if elapsed > 0 else 0
        eta = (n_rows - current_idx) / rate if rate > 0 else 0

        # Memory usage
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
        except:
            memory_mb = 0

        # Fingerprint stats
        n_fingerprints = len(self.fingerprint_signatures)
        avg_signature_size = np.mean(
            [len(sig) for sig in self.fingerprint_signatures.values()]) if n_fingerprints > 0 else 0

        print(f'Processed: {current_idx:,}/{n_rows:,} ({current_idx / n_rows * 100:.1f}%), '
              f'elapsed: {elapsed:.0f}s, rate: {rate:.0f}/s, ETA: {eta:.0f}s, '
              f'Memory: {memory_mb:.0f}MB, Fingerprints: {n_fingerprints:,}, '
              f'Avg sig size: {avg_signature_size:.1f}')

    def get_fingerprint_analysis(self):
        """Get analysis of fingerprint signatures for debugging/optimization."""
        analysis = {
            'total_fingerprints': len(self.fingerprint_signatures),
            'signature_sizes': [len(sig) for sig in self.fingerprint_signatures.values()],
            'feature_usage': {},
            'discriminative_features': {}
        }

        # Analyze feature usage across all fingerprints
        for feature in self.feature_to_fingerprints:
            total_values = sum(len(fps) for fps in self.feature_to_fingerprints[feature].values())
            unique_values = len(self.feature_to_fingerprints[feature])
            analysis['feature_usage'][feature] = {
                'unique_values': unique_values,
                'total_fingerprint_associations': total_values,
                'avg_fingerprints_per_value': total_values / unique_values if unique_values > 0 else 0
            }

        return analysis

# Main interface functions
def process_fingerprints_smart(df, initial_anchor_feature, search_space_reducers,
                               final_identification_features, debug_anchor_value=None):
    """
    Main interface for smart fingerprint processing.

    Args:
        df: Input DataFrame (must be sorted by time)
        initial_anchor_feature: Primary feature for quick matching
        search_space_reducers: Features to narrow down candidates
        final_identification_features: Features for final matching
        debug_anchor_value: Optional anchor value to track for debugging (e.g., "70f16ffc4d6cf98a")

    Returns:
        DataFrame with fingerprint matching results
    """
    processor = SmartFingerprintProcessor(debug_anchor_value=debug_anchor_value)

    result_df = processor.process_fingerprints_smart(
        df, initial_anchor_feature, search_space_reducers, final_identification_features
    )

    # Print final analysis
    analysis = processor.get_fingerprint_analysis()
    print(f"\nFinal Analysis:")
    print(f"Total unique fingerprints: {analysis['total_fingerprints']:,}")
    print(f"Average signature size: {np.mean(analysis['signature_sizes']):.1f}")
    print(
        f"Signature size range: {min(analysis['signature_sizes']) if analysis['signature_sizes'] else 0}-{max(analysis['signature_sizes']) if analysis['signature_sizes'] else 0}")

    return result_df


# Backward compatibility
def process_fingerprints(df, initial_anchor_feature, search_space_reducers, final_identification_features):
    """Backward compatible interface."""
    return process_fingerprints_smart(df, initial_anchor_feature, search_space_reducers,
                                      final_identification_features)


def analyze_dataset_characteristics(df, feature_columns):
    """
    Analyze dataset to recommend optimal feature ordering and processing strategy.
    """
    analysis = {}
    n_rows = len(df)

    print("Analyzing dataset characteristics...")

    for feature in feature_columns:
        series = df[feature].dropna()
        if len(series) == 0:
            continue

        unique_count = series.nunique()
        total_count = len(series)
        selectivity = unique_count / total_count if total_count > 0 else 0
        null_ratio = df[feature].isnull().sum() / n_rows

        # Analyze value distribution
        value_counts = series.value_counts()
        top_10_coverage = value_counts.head(10).sum() / total_count if total_count > 0 else 0

        analysis[feature] = {
            'unique_values': unique_count,
            'total_values': total_count,
            'selectivity': selectivity,
            'null_ratio': null_ratio,
            'top_10_coverage': top_10_coverage,  # How much of data is covered by top 10 values
            'recommendation': 'high_discriminative' if selectivity > 0.1 else
            'medium_discriminative' if selectivity > 0.01 else 'low_discriminative'
        }

    # Sort by selectivity
    sorted_features = sorted(analysis.items(), key=lambda x: x[1]['selectivity'], reverse=True)

    print("\nFeature Analysis (sorted by selectivity):")
    print(f"{'Feature':<30} {'Selectivity':<12} {'Unique':<10} {'Null%':<8} {'Recommendation'}")
    print("-" * 80)

    for feature, stats in sorted_features:
        print(f"{feature:<30} {stats['selectivity']:<12.4f} {stats['unique_values']:<10,} "
              f"{stats['null_ratio'] * 100:<8.1f} {stats['recommendation']}")

    return analysis


def process_csv_fingerprints(csv_file_path, config):
    """
    Complete workflow for processing CSV files with smart fingerprint matching.

    Args:
        csv_file_path (str): Path to your CSV file
        config (dict): Configuration with feature columns and settings

    Returns:
        pd.DataFrame: DataFrame with fingerprint results
    """

    print(f"Loading CSV file: {csv_file_path}")

    cols_to_load = config.get('columns_to_load')
    if cols_to_load:
        df = pd.read_csv(csv_file_path, low_memory=False, dtype=str, usecols=cols_to_load)
    else:
        df = pd.read_csv(csv_file_path, low_memory=False, dtype=str)

    print(f"Loaded {len(df):,} rows with {len(df.columns)} columns")

    if config.get('timestamp_column'):
        print(f"Sorting by timestamp column: {config['timestamp_column']}")
        df = df.sort_values(config['timestamp_column']).reset_index(drop=True)
    else:
        print("WARNING: No timestamp column specified. Using existing order.")
        print("Make sure your data is already sorted by time for correct fingerprint logic!")

    # Step 2: Analyze dataset characteristics (optional but recommended)
    if config.get('analyze_features', True):
        print("\n" + "=" * 50)
        print("ANALYZING DATASET CHARACTERISTICS")
        print("=" * 50)

        all_feature_cols = [config['initial_anchor_feature']] + \
                           config['search_space_reducers'] + \
                           config['final_identification_features']

        analysis = analyze_dataset_characteristics(df, all_feature_cols)

        # Show recommendations
        print(f"\nRECOMMENDATIONS:")
        high_discriminative = [f for f, stats in analysis.items()
                               if stats['recommendation'] == 'high_discriminative']
        print(f"High discriminative features (good for reducers): {high_discriminative}")

    # Step 3: Process fingerprints
    print("\n" + "=" * 50)
    print("STARTING FINGERPRINT PROCESSING")
    print("=" * 50)

    result_df = process_fingerprints_smart(
        df=df,
        initial_anchor_feature=config['initial_anchor_feature'],
        search_space_reducers=config['search_space_reducers'],
        final_identification_features=config['final_identification_features'],
        debug_anchor_value=None,
    )

    # Step 4: Results summary
    print("\n" + "=" * 50)
    print("PROCESSING RESULTS")
    print("=" * 50)

    total_rows = len(result_df)
    new_fingerprints = result_df['is_new_fingerprint'].sum()
    matched_fingerprints = total_rows - new_fingerprints
    unique_fingerprints = result_df['new_fingerprint'].nunique()

    print(f"Total rows processed: {total_rows:,}")
    print(f"New fingerprints created: {new_fingerprints:,}")
    print(f"Rows matched to existing: {matched_fingerprints:,}")
    print(f"Unique fingerprints total: {unique_fingerprints:,}")
    print(f"Match rate: {(matched_fingerprints / total_rows) * 100:.1f}%")

    # Show match feature breakdown
    match_breakdown = result_df['match_at_feature'].value_counts()
    print(f"\nMatches by feature:")
    for feature, count in match_breakdown.items():
        print(f"  {feature}: {count:,} ({count / total_rows * 100:.1f}%)")

    result_df.to_csv('niyo_fraud_data_new_fp_new.csv', index=False)

    print(f'Finished processing {total_rows:} rows.')

    return result_df


if __name__ == "__main__":

    # Device Fingerprinting
    # '''
    file_path = 'niyo_fraud_data_for_fgp_new.csv'
    sample_df = pd.read_csv(file_path, nrows=100, low_memory=False, dtype=str)
    matchers = [col for col in sample_df.columns if 'matcher_' in col and '_fallback_' not in col and '_last_check_' not in col]
    matchers.extend([col for col in sample_df.columns if 'matcher_fallback_' in col])
    matchers.extend([col for col in sample_df.columns if 'matcher_last_check_' in col])
    search_space_reducers = [col for col in sample_df.columns if 'reducer_' in col]

    print(f'Reducers: {search_space_reducers}')
    print(f'Matchers: {matchers}')

    MOBILE_DEVICE_CONFIG = {
        'timestamp_column': 'timestamp',
        'initial_anchor_feature': 'anchor_android_id',
        'search_space_reducers': search_space_reducers,
        'final_identification_features': matchers,
        'analyze_features': False,
        'columns_to_load': [
            "timestamp", "deviceId", "userId",
            "modelName", "manufacturerName", "bootTime", "bootCount", "wifiSSID",
            "latitude", "longitude", "totalInternalStorageSpace.total",
            "totalInternalStorageSpace.available", "lastFactoryResetOrDeviceUpdateTime",
            "minTimeByInstalledPackages", "requestId", "appSessionId", "androidVersion",

            # "androidId",  "adId", "gsfId", "drmId", "systemPropertiesParsed",
            # "isVpn", "isProxy", "isEmulator", "isAppCloned", "isGeoSpoofed", "isRooted", "usbDebugState",
            # "isHooking", "isAppTampering", "developerEnabled", "newDevice", "useCableState", "isScreenBeingMirrored",
            # "simIds", "totalSimUsed", "carrierCountry", "carrierName", "networkType", "systemBootDigests",

        ] + [
            'anchor_android_id', 'reducer_product_sensor_hash', 'reducer_camera_sensor_hash',
            'reducer_system_property_hash', 'matcher_boot_hw_soc_id', 'matcher_boot_unique_no', 'matcher_debug_gps_hash',
            'matcher_oplus_fingerprint_qrcode', 'matcher_service_wifi_mac', 'matcher_sys_miui_sno',
            'matcher_vivo_hash', 'matcher_fallback_ad_id', 'matcher_fallback_gsf_id', 'matcher_last_check_drm_id'
        ]
    }
    process_csv_fingerprints(file_path, MOBILE_DEVICE_CONFIG)
    # '''

    # Collision Identification

    '''
    file_path = 'shaadi_new_fingerprints_on_input_data_20250923_20251003.csv'
    result = pd.read_csv(file_path, low_memory=False, dtype=str)

    def edit_file_for_better_check_on_sheets(df):
        df.rename(columns={
                'request.deviceParams.deviceIdRawData.hardwareFingerprintRawData.manufacturerName': 'manufacturerName',
                'request.deviceParams.deviceIdRawData.hardwareFingerprintRawData.modelName': 'modelName',
            }, inplace=True)
        df.drop(columns=['systemPropertiesParsed', ], inplace=True)
        col_seq = [
            'timestamp', 'new_fingerprint', 'deviceId', 'manufacturerName', 'modelName', 'bootTime', 'bootCount',
            'wifiSSID', 'lastFactoryResetOrDeviceUpdateTime', 'minTimeByInstalledPackages', 'deviceLatitude',
            'deviceLongitude', 'userId', 'anchor_android_id', 'reducer_product_sensor_hash',
            'reducer_camera_sensor_hash', 'reducer_system_property_hash']
        other_cols = [x for x in df.columns if x not in col_seq]
        final_seq = col_seq + other_cols
        df = df[final_seq].copy()
        return df

    collision_ids = result[['new_fingerprint', 'deviceId']].groupby('new_fingerprint').agg(
        'nunique').reset_index()
    collision_ids = collision_ids.loc[collision_ids['deviceId'] > 1][
        'new_fingerprint'].tolist()
    if len(collision_ids) > 0:
        filtered_result = result.loc[result['new_fingerprint'].isin(collision_ids)].copy()
        filtered_result = edit_file_for_better_check_on_sheets(filtered_result)
        print(f'total collision ids: {len(collision_ids)}')
        print(f'shape of filtered result df: {filtered_result.shape}')
        print(f'head of filtered result df: \n{filtered_result.sort_values(by=["deviceId", "timestamp"], ascending=[True, True]).head().to_string(index=False)}\n')
        filtered_result.to_csv(
            'shaadi_new_fingerprints_on_input_data_collision_cases_20250923_20251003.csv',
            index=False)
    else:
        print('No collisions detected.')
    '''