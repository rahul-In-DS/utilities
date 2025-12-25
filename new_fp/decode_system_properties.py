import pandas as pd
import sys
import os
from tqdm import tqdm
tqdm.pandas()

# Add the current directory to sys.path to allow importing from decode_system_boot_properties.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from decode_system_boot_properties import reconstruct_data
except ImportError:
    # If standard import fails, try importing assuming we are in the directory
    import decode_system_boot_properties
    reconstruct_data = decode_system_boot_properties.reconstruct_data

def safe_reconstruct(val):
    if pd.isna(val) or val == "":
        return {}
    try:
        return reconstruct_data(val)
    except Exception as e:
        # print(f"Error parsing value: {val[:20]}... {e}")
        return {}

def main():
    input_csv = 'total_data_niyo_2025_fraud.csv'
    output_csv = 'total_data_niyo_2025_fraud_decoded.csv'
    
    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv)
    
    print("Parsing systemProperties...")
    # Apply the reconstruction logic
    # We use a lambda or list comprehension for better performance if possible, but apply is easiest
    print("Reconstructing data...")
    parsed_series = df['systemProperties'].progress_apply(safe_reconstruct)
    
    # List of columns requested by the user
    target_columns = [
        'camera.sensor.frontMain.fuseID', 'camera.sensor.rearMain.dualfuseID',
        'camera.sensor.rearMain.fuseID', 'camera.sensor.rearUltra.fuseID',
        'gsm.serial', 'oplus.fingerprint.qrcode.value',
        'persist.service.wifi.mac', 'persist.sys.bluetooth.dump.zip.name',
        'persist.sys.gsensor_cal_xyz', 'persist.sys.gyroscope_cal_xyz',
        'persist.sys.light.full_color_cali', 'persist.sys.light.location_cali1',
        'persist.sys.light.location_cali2',
        'persist.sys.light.location_cali_position',
        'persist.sys.light.low_color_cali', 'persist.sys.lite.uid',
        'persist.sys.miui.sno', 'persist.sys.oplus.watchdogtrace',
        'persist.sys.ota.boot_completed_time', 'persist.sys.panic.file',
        'persist.sys.send.file', 'persist.sys.zram.total_writes',
        'persist.vendor.camera.oisalgoparam', 'persist.vendor.gsensor_cal_xyz',
        'persist.vendor.gyroscope_cal_xyz',
        'persist.vendor.radio.imsconfig.hashed_last_iccid1',
        'persist.vendor.radio.imsconfig.hashed_last_iccid2',
        'persist.vendor.radio.last_sim1', 'persist.vendor.radio.last_sim2',
        'persist.vendor.radio.ut.imsi.info',
        'persist.vendor.radio.ut.xui.info_1',
        'persist.vendor.radio.ut.xui.info_2', 'persist.vendor.sys.fp.uid',
        'persist.vivo.initial_system_time_millis', 'persist.vivo.reboot.splog',
        'persist.vivo.systemSwtTime', 'persist.vivo.systemswttime',
        'persist.vivo.vchg_startup_wizard_time', 'ril.rfcal_date',
        'ro.boot.chipecid', 'ro.boot.chipid', 'ro.boot.hw.soc.id',
        'ro.boot.label_time', 'ro.boot.uniqueno', 'ro.product.device',
        'ro.product.model', 'ro.product.odm.device', 'ro.product.odm.model',
        'ro.product.vendor.device', 'sys.theia.target_uuid',
        'vendor.camera.sensor.f.fuseid',
        'vendor.camera.sensor.frontMain.fuseID',
        'vendor.camera.sensor.frontmain.fuseid',
        'vendor.camera.sensor.m.fuseid',
        'vendor.camera.sensor.rearDepth.fuseID',
        'vendor.camera.sensor.rearMacro.fuseID',
        'vendor.camera.sensor.rearMain.fuseID',
        'vendor.camera.sensor.rearUltra.fuseID',
        'vendor.camera.sensor.reardepth.fuseid',
        'vendor.camera.sensor.rearmacro.fuseid',
        'vendor.camera.sensor.rearmain.fuseid',
        'vendor.camera.sensor.rearultra.fuseid',
        'vendor.camera.sensor.u.fuseid', 'vendor.camera.sensor.w.fuseid',
        'vendor.debug.gps.c0', 'vendor.debug.gps.c1', 'vendor.gsm.serial'
    ]
    
    print("Extracting target columns...")
    # Create a new DataFrame with the target columns
    # We iterate over the parsed dicts and extract keys
    
    # Optimization: using list comprehension is faster than apply for extraction
    extracted_data = []
    for d in tqdm(parsed_series, desc="Extracting columns"):
        row_data = {col: d.get(col, None) for col in target_columns}
        extracted_data.append(row_data)
        
    extracted_df = pd.DataFrame(extracted_data)
    
    # Concatenate the original dataframe (dropping the systemProperties column) with the extracted columns
    # Re-using the index from the original dataframe to ensure correct alignment
    df_dropped = df.drop(columns=['systemProperties'])
    result_df = pd.concat([df_dropped, extracted_df], axis=1)
    
    print(f"Saving to {output_csv}...")
    result_df.to_csv(output_csv, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
