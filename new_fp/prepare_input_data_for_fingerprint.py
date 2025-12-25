import os
import h3
import csv
import ast
import json
import hashlib
import pandas as pd
from tqdm import tqdm
from typing import Dict, List, Any, Optional


def round_to_nearest_base(number, base):
  """
  Rounds a number to the nearest multiple of base.
  This implementation uses Python's standard round() which follows
  "round half to even" but effectively acts like "round half up" when scaled
  to the nearest multiple of base.
  """
  return round(number / base) * base



def safe_load_json_string(data_stream, default = None):
    try:
        return json.loads(data_stream)
    except json.decoder.JSONDecodeError:
        try:
            return ast.literal_eval(data_stream)
        except (KeyError, ValueError, SyntaxError, IndexError):
            return default


def prepare_hash(row_dict: Dict[str, Any], cols: List[str]) -> Optional[str]:
    data_parts = []

    for col in cols:
        value = row_dict.get(col)
        if pd.notna(value) and str(value).lower() not in ['nan', 'none', '', 'inf']:
            data_parts.append(str(value))

    if not data_parts:
        return None

    data = ''.join(data_parts)
    return hashlib.sha256(data.encode()).hexdigest()


def process_row(row: Dict) -> Optional[Dict]:
    """Process a single row with all transformations"""

    # Parse systemPropertiesParsed JSON
    sys_props_str = row.get('systemPropertiesParsed', '')
    sys_props = safe_load_json_string(sys_props_str, {})

    if not sys_props:  # Skip rows where systemPropertiesParsed is invalid
        return None

    # Merge system properties into row
    row.update(sys_props)

    # Handle edge cases for oplus.fingerprint.qrcode.value
    qrcode_val = row.get('oplus.fingerprint.qrcode.value')
    if qrcode_val and len(str(qrcode_val)) > 5:
        if '00000' in str(qrcode_val)[-1:-6:-1]:
            row['oplus.fingerprint.qrcode.value'] = None

    # Handle vendor.debug.gps.c0
    gps_c0 = row.get('vendor.debug.gps.c0')
    try:
        if gps_c0 and (float(gps_c0) == 0 or float(gps_c0) == float('inf')):
            row['vendor.debug.gps.c0'] = None
    except (ValueError, TypeError):
        pass

    # Handle vendor.debug.gps.c1
    gps_c1 = row.get('vendor.debug.gps.c1')
    try:
        if not row.get('vendor.debug.gps.c0') or (gps_c1 and (float(gps_c1) == 0 or float(gps_c1) == float('inf'))):
            row['vendor.debug.gps.c1'] = None
    except (ValueError, TypeError):
        pass

    # Handle persist.vivo fields
    vivo_initial = row.get('persist.vivo.initial_system_time_millis')
    try:
        if vivo_initial and (float(vivo_initial) == 0 or float(vivo_initial) == float('inf')):
            row['persist.vivo.initial_system_time_millis'] = None
    except (ValueError, TypeError):
        pass

    vivo_wizard = row.get('persist.vivo.vchg_startup_wizard_time')
    try:
        if not row.get('persist.vivo.initial_system_time_millis') or (
                vivo_wizard and (float(vivo_wizard) == 0 or float(vivo_wizard) == float('inf'))):
            row['persist.vivo.vchg_startup_wizard_time'] = None
    except (ValueError, TypeError):
        pass

    # Handle ro.boot.hw.soc.id
    soc_id = row.get('ro.boot.hw.soc.id')
    if soc_id and len(str(soc_id)) < 4 and 'inf' in str(soc_id):
        row['ro.boot.hw.soc.id'] = None

    # Prepare hashes
    row['reducer_product_sensor_hash'] = prepare_hash(row, [
        "ro.product.vendor.device", "ro.product.device", "ro.product.odm.model",
        "ro.product.model", "ro.product.odm.device", "persist.sys.gsensor_cal_xyz",
        "gyroscope_cal_xyz", "persist.vendor.gsensor_cal_xyz", "persist.vendor.gyroscope_cal_xyz"
    ])

    row['reducer_camera_sensor_hash'] = prepare_hash(row, [
        "vendor.camera.sensor.u.fuseid",
        "vendor.camera.sensor.m.fuseid",
        "vendor.camera.sensor.w.fuseid",
        "vendor.camera.sensor.f.fuseid"
    ])

    row['reducer_system_property_hash'] = prepare_hash(row, [
        "ril.rfcal_date", "gsm.serial", "persist.vendor.camera.oisAlgoParam"
    ])

    row['matcher_debug_gps_hash'] = prepare_hash(row, [
        "vendor.debug.gps.c0", "vendor.debug.gps.c1"
    ])

    row['matcher_vivo_hash'] = prepare_hash(row, [
        "persist.vivo.initial_system_time_millis", "persist.vivo.vchg_startup_wizard_time"
    ])

    # row['matcher_last_check_model_boot_time'] = prepare_hash(row, [
    #     "bootTime", "modelName"
    # ])

    def get_h3_grid(data):
        lat, lng = data['latitude'], data['longitude']

        try:
            lat = None if pd.isna(lat) else float(lat)
        except ValueError:
            lat = None

        try:
            lng = None if pd.isna(lng) else float(lng)
        except ValueError:
            lng = None

        if lat and lng:
            return h3.latlng_to_cell(lat, lng, res=10)
        else:
            return None

    def get_memory_available_percentage(data):

        total_mem, available_mem = (data['totalInternalStorageSpace.total'],
                                    data['totalInternalStorageSpace.available'])
        total_mem = None if pd.isna(total_mem) else float(total_mem)
        available_mem = None if pd.isna(available_mem) else float(available_mem)

        if total_mem and available_mem:
            return str(round_to_nearest_base((available_mem*100) / total_mem, base=4))
        else:
            return None


    row['uber_h3_grid'] = get_h3_grid(row)
    row['memory_available_pct'] = get_memory_available_percentage(row)

    # Apply column renaming
    column_mapping = {
        "ro.boot.hw.soc.id": "matcher_boot_hw_soc_id",
        "persist.sys.miui.sno": "matcher_sys_miui_sno",
        "ro.boot.uniqueno": "matcher_boot_unique_no",
        "persist.service.wifi.mac": "matcher_service_wifi_mac",
        "oplus.fingerprint.qrcode.value": "matcher_oplus_fingerprint_qrcode",
        "androidId": "anchor_android_id",
        "adId": "matcher_fallback_ad_id",
        "gsfId": "matcher_fallback_gsf_id",
        "drmId": "matcher_last_check_drm_id",
    }

    for old_col, new_col in column_mapping.items():
        if old_col in row:
            row[new_col] = row.pop(old_col)

    return row


def get_file_line_count(file_path: str) -> int:
    """Efficiently count lines in file for progress bar"""
    print(f"Counting lines in {file_path}...")
    count = 0
    with open(file_path, 'rb') as f:
        for _ in tqdm(f, desc="Counting lines", unit=" lines", unit_scale=True):
            count += 1
    return count - 1  # Subtract 1 for header


def stream_process_csv(input_file: str, output_file: str, chunk_size: int = 10000, sample_rows: int = 1000):
    """
    Stream process large CSV file in chunks
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        chunk_size: Number of rows to process before writing (controls memory usage)
        sample_rows: Number of rows to sample to discover all possible columns
    """

    cols_required = [
        "timestamp", "deviceId", "androidId", "userId", "adId", "gsfId", "drmId",
        "packageName", "sha1", "modelName", "manufacturerName", "bootTime", "bootCount",
        "wifiSSID", "latitude", "longitude", "totalInternalStorageSpace.total",
        "totalInternalStorageSpace.available", "lastFactoryResetOrDeviceUpdateTime",
        "minTimeByInstalledPackages", "systemPropertiesParsed", "systemBootDigests",
        "requestId", "appSessionId", "androidVersion", "carrierCountry", "carrierName",
        "networkType", "usbDebugState", "useCableState", "isScreenBeingMirrored",
        "isVpn", "isProxy", "isEmulator", "isAppCloned", "isGeoSpoofed", "isRooted",
        "isHooking", "isAppTampering", "developerEnabled", "newDevice", "simIds", "totalSimUsed"
    ]

    print(f"\n{'=' * 70}")
    print(f"STEP 1: Column Discovery")
    print(f"{'=' * 70}")
    print(f"Scanning first {sample_rows:,} rows to discover all possible columns...")

    # First pass: discover all possible columns
    all_fieldnames = set()
    with open(input_file, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        available_cols = [col for col in cols_required if col in reader.fieldnames]

        with tqdm(total=sample_rows, desc="Discovering columns", unit=" rows") as pbar:
            for i, row in enumerate(reader):
                if i >= sample_rows:
                    break

                filtered_row = {k: row.get(k, '') for k in available_cols}
                processed_row = process_row(filtered_row)

                if processed_row:
                    all_fieldnames.update(processed_row.keys())

                pbar.update(1)

    # Convert to sorted list for consistent column ordering
    all_fieldnames = sorted(list(all_fieldnames))
    print(f"✓ Found {len(all_fieldnames)} unique columns")

    print(f"\n{'=' * 70}")
    print(f"STEP 2: Full File Processing")
    print(f"{'=' * 70}")

    # Get total line count for accurate progress bar
    total_lines = get_file_line_count(input_file)
    print(f"Total rows to process: {total_lines:,}\n")

    processed_rows = []
    total_rows = 0
    valid_rows = 0

    with open(input_file, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        available_cols = [col for col in cols_required if col in reader.fieldnames]

        with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=all_fieldnames, extrasaction='ignore')
            writer.writeheader()

            with tqdm(total=total_lines, desc="Processing rows", unit=" rows",
                      unit_scale=False, dynamic_ncols=True) as pbar:

                for row in reader:
                    total_rows += 1

                    filtered_row = {k: row.get(k, '') for k in available_cols}
                    processed_row = process_row(filtered_row)

                    if processed_row:
                        valid_rows += 1
                        # Ensure all fields exist (fill missing with None)
                        complete_row = {field: processed_row.get(field, None) for field in all_fieldnames}
                        processed_rows.append(complete_row)

                        if len(processed_rows) >= chunk_size:
                            writer.writerows(processed_rows)
                            outfile.flush()
                            processed_rows = []

                    # Update progress bar with additional stats
                    pbar.set_postfix({
                        'valid': f"{valid_rows:,}",
                        'valid_rate': f"{(valid_rows / total_rows * 100):.1f}%"
                    })
                    pbar.update(1)

                # Write remaining rows
                if processed_rows:
                    writer.writerows(processed_rows)

    print(f"\n{'=' * 70}")
    print(f"✓ COMPLETED!")
    print(f"{'=' * 70}")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Valid rows written:   {valid_rows:,}")
    print(f"Invalid/skipped:      {total_rows - valid_rows:,}")
    print(f"Output file:          {output_file}")
    print(f"Output file size:     {os.path.getsize(output_file) / (1024 ** 3):.2f} GB")
    print(f"{'=' * 70}\n")

    return total_rows, valid_rows


if __name__ == "__main__":
    input_csv = "for_input_file_new.csv"
    output_csv = "niyo_fraud_data_for_fgp_new.csv"

    total, valid = stream_process_csv(input_csv, output_csv, chunk_size=20000, sample_rows=20000)