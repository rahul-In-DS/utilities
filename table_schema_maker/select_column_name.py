import json

def get_flattened_response(resp_obj):
    def flatten_dict(d, parent_key=''):
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict) and len(v) > 0:
                items.update(flatten_dict(v, new_key))
            else:
                items[new_key] = v
        return items

    def update_key(key):
        # key = key.replace('account_details.', '')
        # key = key.replace('user_exist', 'account_exist')
        # return key.replace('.', '_')
        return key

    flattened_resp = flatten_dict(resp_obj)
    flattened_resp = {update_key(k): v for k, v in flattened_resp.items()}

    return flattened_resp

def prepare_query(response_data):
    missing_keys = [
    ]

    def format_key(key_name):

        keywords = ['text', 'min', 'system', 'default', 'version', 'min',
                    'max', 'left', 'right', 'Features']

        key_name = str(key_name)
        splits = key_name.split('.')

        if len(set(splits) & set(keywords)):
            common_keywords = set(splits) & set(keywords)
            final_key = ''
            for split in splits:
                if split in common_keywords:
                    if final_key == '':
                        final_key = final_key + f'"{split}"'
                    else:
                        final_key = final_key + f'."{split}"'
                else:
                    if final_key == '':
                        final_key = final_key + f'{split}'
                    else:
                        final_key = final_key + f'.{split}'
            return final_key
        else:
            return key_name

    print(f'SELECT')
    flat_record = get_flattened_response(response_data)
    for key in flat_record.keys():
        if key in missing_keys:
            continue
        key_query = format_key(key)
        print(f'\tt2.{key_query} as "{key}",')

    # print(f'FROM hey_you_db.web_fingerprint_table')
    # print(f"WHERE tenant='abfl-web-prod'")
    # print(f"\tAND date >= date('2025-04-21')")
    # print(f"\tAND date <= date('2025-05-25')")

if __name__ == "__main__":

    record = {
  "requestId": "d96b1aae-48e8-4f91-acac-61750a8f2fdf",
  "tenantId": "snapmint-di-prod",
  "versionCode": "26",
  "request": {
    "requestId": "d96b1aae-48e8-4f91-acac-61750a8f2fdf",
    "appSessionId": "edc24083-2298-4473-afe5-2b9d2a642ef2",
    "deviceParams": {
      "androidData": {
        "packageName": "com.snapmint.customerapp",
        "deviceId": "d9950404fdd75a32",
        "isEmulator": "false",
        "isProxy": "false",
        "isRemoteControlProvider": "false",
        "isVpn": "false",
        "operatorName": "JIO 4G",
        "installedPath": "/data/user/0/com.snapmint.customerapp/files",
        "build": {
          "brand": "realme",
          "device": "RE5C6CL1",
          "fingerprint": "realme/RMX3785IN/RE5C6CL1:15/AP3A.240617.008/T.R4T2.1ddc98b_6788-f67f1:user/release-keys",
          "model": "RMX3785",
          "manufacturer": "realme",
          "hardware": "mt6835",
          "product": "RMX3785IN",
          "board": "k6835v1_64",
          "bootloader": "unknown",
          "androidSecurityPatch": "2025-05-01",
          "buildId": "AP3A.240617.008",
          "buildNumber": "RMX3785_15.0.0.940(EX01)"
        },
        "sha1": "86:a3:58:5c:eb:d5:91:d7:27:d9:2b:f8:61:4b:5b:51:db:c3:73:7c",
        "developerEnabled": "false",
        "isRooted": "false",
        "isGeoSpoofed": "false",
        "isGooglePlayServicesPresent": "true",
        "rootData": {
          "xposedHooks": "false",
          "dualWorkProfile": "NOT_FOUND",
          "twrp": "NOT_FOUND",
          "xposedModules": [],
          "dangerousApps": "NOT_FOUND",
          "rootPackages": "NOT_FOUND",
          "xprivacyLua": "NOT_FOUND"
        },
        "isHooking": "false",
        "isAppTampering": "false",
        "isDeviceLocked": "true",
        "isCloned": "false",
        "nfcAvailable": "false",
        "nfcEnabled": "false",
        "onCall": "false",
        "audioMuteStatus": "false",
        "proximitySensor": "true",
        "isScreenBeingMirrored": "false",
        "explanationData": {
          "rootMessage": {
            "detectionValue": "false",
            "testKeyPresent": "false",
            "rootManagementApps": [],
            "binarySUList": [],
            "binaryBusyBoxList": [],
            "binaryMagiskList": [],
            "dangerousPropsList": [],
            "rwPathsList": [],
            "suExists": "false",
            "rootCloakingApps": [],
            "detectMagisk": "false",
            "rootDetectionFromNative": "false"
          },
          "hookingMessage": ""
        }
      },
      "deviceIdRawData": {
        "androidDeviceIDs": {
          "androidId": "d9950404fdd75a32",
          "gsfId": "",
          "widevineDrmId": "1b03c01ce99c35ab821862fd8ad81197abbaf636c8ab92f20c06be8aa421a3e1",
          "adsId": "593fd5ac-a477-48b1-98df-89aa4d176366"
        },
        "deviceStateRawData": {
          "adbEnabled": "0",
          "developmentSettingsEnabled": "0",
          "transitionAnimationScale": "1.0",
          "windowAnimationScale": "1.0",
          "dataRoamingEnabled": "",
          "accessibilityEnabled": "0",
          "defaultInputMethod": "com.google.android.inputmethod.latin/com.android.inputmethod.latin.LatinIME",
          "rttCallingMode": "0",
          "touchExplorationEnabled": "0",
          "alarmAlertPath": "content://media/internal/audio/media/872?title=ringtone_008&canonical=1",
          "dateFormat": "24",
          "endButtonBehaviour": "2",
          "fontScale": "1.0",
          "screenOffTimeout": "30000",
          "time12Or24": "24",
          "isPinSecurityEnabled": "true",
          "fingerprintSensorStatus": "enabled",
          "availableLocales": [
            "af",
            "af-ZA",
            "ar",
            "ar-EG",
            "ar-IL",
            "ar-XB",
            "as",
            "as-IN",
            "az",
            "az-AZ",
            "bg",
            "bg-BG",
            "bn",
            "bn-BD",
            "bn-IN",
            "bo-CN",
            "ca",
            "ca-ES",
            "cs",
            "cs-CZ",
            "da",
            "da-DK",
            "de",
            "de-AT",
            "de-CH",
            "de-DE",
            "el",
            "el-GR",
            "en",
            "en-AU",
            "en-CA",
            "en-GB",
            "en-IE",
            "en-IN",
            "en-NZ",
            "en-SG",
            "en-US",
            "en-XA",
            "en-XC",
            "es",
            "es-CO",
            "es-ES",
            "es-MX",
            "es-US",
            "et",
            "et-EE",
            "eu",
            "eu-ES",
            "fa",
            "fa-IR",
            "fi",
            "fi-FI",
            "fil",
            "fil-PH",
            "fr",
            "fr-CH",
            "fr-FR",
            "ga-IE",
            "gl",
            "gl-ES",
            "gu",
            "gu-IN",
            "hi",
            "hi-IN",
            "hr",
            "hr-HR",
            "hu",
            "hu-HU",
            "hy",
            "hy-AM",
            "in",
            "in-ID",
            "it",
            "it-CH",
            "it-IT",
            "iw",
            "iw-IL",
            "ja",
            "ja-JP",
            "ka",
            "ka-GE",
            "kea-CV",
            "kk",
            "kk-CN",
            "kk-KZ",
            "km",
            "km-KH",
            "kn",
            "kn-IN",
            "ko",
            "ko-KR",
            "lb-LU",
            "lo",
            "lo-LA",
            "lt",
            "lt-LT",
            "lv",
            "lv-LV",
            "ml",
            "ml-IN",
            "mr",
            "mr-IN",
            "ms",
            "ms-MY",
            "mt-MT",
            "my",
            "my-MM",
            "my-ZG",
            "nb",
            "nb-NO",
            "ne",
            "ne-NP",
            "nl",
            "nl-NL",
            "or",
            "or-IN",
            "pa",
            "pa-IN",
            "pl",
            "pl-PL",
            "pt",
            "pt-BR",
            "pt-PT",
            "ro",
            "ro-RO",
            "ru",
            "ru-RU",
            "si",
            "si-LK",
            "sk",
            "sk-SK",
            "sl",
            "sl-SI",
            "sr",
            "sr-Latn",
            "sr-RS",
            "sv",
            "sv-SE",
            "sw",
            "sw-KE",
            "ta",
            "ta-IN",
            "te",
            "te-IN",
            "th",
            "th-TH",
            "tr",
            "tr-TR",
            "ug-CN",
            "uk",
            "uk-UA",
            "ur",
            "ur-PK",
            "uz",
            "uz-UZ",
            "vi",
            "vi-VN",
            "zh-CN",
            "zh-HK",
            "zh-TW",
            "zu",
            "zu-ZA"
          ],
          "regionCountry": "GB",
          "defaultLanguage": "en",
          "timezone": "India Standard Time",
          "bootTime": 174957414009,
          "screenResolution": "1080x2290",
          "brightnessMode": "manual",
          "bootloader": "unknown",
          "simInfoList": [],
          "displayDensity": "480",
          "displayFormat": "LONG",
          "displaySize": "NORMAL",
          "ist": "2025-06-11 14:00:47",
          "defaultBrowser": "Chrome",
          "buildTime": 1747452935000,
          "carrierCountry": "in",
          "carrierName": "JIO 4G",
          "networkType": "WIFI",
          "deviceOrientation": "Portrait",
          "useCableState": "USB_DISCONNECTED",
          "usbDebugState": "USB_DEBUGGING_DISABLED",
          "bootCount": "207",
          "dataType": "WIFI",
          "deviceIp": "192.168.1.44",
          "dnsIP": "[103.62.236.85, 103.62.236.84]",
          "wifiSSID": "\"Trends footwear\"",
          "bootLoaderState": "BOOTLOADER_STATE_LOCKED",
          "isPowerSaveMode": "false",
          "wifiLinkSpeed": "6 Mbps",
          "isFastInternet": "false"
        },
        "hardwareFingerprintRawData": {
          "manufacturerName": "realme",
          "modelName": "RMX3785",
          "totalRAM": {
            "total": 8003399680,
            "available": 3543724032,
            "isLow": "false"
          },
          "totalInternalStorageSpace": {
            "total": 115911655424,
            "available": 48385368064
          },
          "totalExternalStorageSpace": {
            "total": 115911655424,
            "available": 48385368064
          },
          "procCpuInfo": {
            "BogoMIPS": "26.00",
            "CPU implementer": "0x41",
            "CPU architecture": "8",
            "CPU variant": "0x4",
            "CPU revision": "0",
            "Features": "fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm lrcpc dcpop asimddp",
            "processor": "7",
            "CPU part": "0xd0b"
          },
          "sensors": [
            {
              "sensorName": "bma4xy acc",
              "vendorName": "bosch"
            },
            {
              "sensorName": "mxg4300 mag",
              "vendorName": "Haechitech"
            },
            {
              "sensorName": "orientation",
              "vendorName": "mtk"
            },
            {
              "sensorName": "oem-pseudo-gyro",
              "vendorName": "virtual_gyro"
            },
            {
              "sensorName": "stk33502 als",
              "vendorName": "SensorTek"
            },
            {
              "sensorName": "stk33502 ps",
              "vendorName": "SensorTek"
            },
            {
              "sensorName": "gravity",
              "vendorName": "mtk"
            },
            {
              "sensorName": "linear_acc",
              "vendorName": "mtk"
            },
            {
              "sensorName": "rot_vec",
              "vendorName": "mtk"
            },
            {
              "sensorName": "uncali_mag",
              "vendorName": "mtk"
            },
            {
              "sensorName": "game_rotvec",
              "vendorName": "mtk"
            },
            {
              "sensorName": "significant",
              "vendorName": "mtk"
            },
            {
              "sensorName": "step_detect",
              "vendorName": "mtk"
            },
            {
              "sensorName": "step_count",
              "vendorName": "mtk"
            },
            {
              "sensorName": "geo_rotvec",
              "vendorName": "mtk"
            },
            {
              "sensorName": "dev_orient",
              "vendorName": "mtk"
            },
            {
              "sensorName": "uncali_acc",
              "vendorName": "mtk"
            },
            {
              "sensorName": "pedo_minute",
              "vendorName": "oplus"
            },
            {
              "sensorName": "stationary",
              "vendorName": "oplus"
            },
            {
              "sensorName": "rotation_detect",
              "vendorName": "oplus"
            },
            {
              "sensorName": "palm_detect",
              "vendorName": "oplus"
            },
            {
              "sensorName": "step_detect_wakeup",
              "vendorName": "mtk"
            }
          ],
          "inputDevices": [
            {
              "name": "Virtual",
              "vendor": "0"
            },
            {
              "name": "mtk-pmic-keys",
              "vendor": "1"
            },
            {
              "name": "touchpanel",
              "vendor": "0"
            },
            {
              "name": "mtk-kpd",
              "vendor": "0"
            },
            {
              "name": "mt6835-mt6377 Headset Jack",
              "vendor": "0"
            },
            {
              "name": "uinput_nav",
              "vendor": "0"
            }
          ],
          "batteryHealth": "good",
          "batteryFullCapacity": "5000.0",
          "cameraList": [
            {
              "cameraName": "0",
              "cameraType": "back",
              "cameraOrientation": "90"
            },
            {
              "cameraName": "1",
              "cameraType": "front",
              "cameraOrientation": "270"
            }
          ],
          "glesVersion": "3.2",
          "abiType": "arm64-v8a",
          "coresCount": 8,
          "audioCurrentVolume": 5,
          "batteryChargeStatus": "Discharging",
          "batteryLevel": "82%",
          "batteryTemperature": "34.9°C",
          "batteryVoltage": "4mV",
          "cpuCount": "8",
          "cpuHash": "7a247511f40f347ad458028802210021fd4cb76908a60e8855cacf1a71964bee",
          "cpuSpeed": "2000.0 MHz",
          "cpuType": "Unknown",
          "deviceHash": "e36409e9a85f3b8748e88d779a57c5113c811dc21aedf36e90f9d1d4825e17d4",
          "biometricStatus": "AVAILABLE",
          "gpuInfo": {
            "renderer": "Mali-G57 MC2",
            "vendor": "ARM",
            "version": "OpenGL ES 3.2 v1.r38p1"
          }
        },
        "installedAppsRawData": {
          "applicationsNamesList": [
            {
              "packageName": "com.jio.media.ondemand",
              "sourceDir": "/data/app/~~XE6on-mrvoD1JIGPOGT6cQ==/com.jio.media.ondemand-lSzjaIsFVatMxrsOPnw1Pg==/base.apk"
            },
            {
              "packageName": "com.truecaller",
              "sourceDir": "/data/app/~~XfUuneDo0FOQT04VOubfhQ==/com.truecaller-oJ-o8HnPFXqonyfT3vT17g==/base.apk"
            },
            {
              "packageName": "com.whereismytrain.android",
              "sourceDir": "/data/app/~~8kcA9MUUeZdF6O2Kdkg2vQ==/com.whereismytrain.android-LgJN5yXWwstXmNmrO0WYww==/base.apk"
            },
            {
              "packageName": "com.realmestore.app",
              "sourceDir": "/data/app/~~U0rTxxX-mGpVVBziHLrEJQ==/com.realmestore.app-P2I_keLS4JLR5Ceqfz3C8Q==/base.apk"
            },
            {
              "packageName": "com.phonepe.app",
              "sourceDir": "/data/app/~~DRbcmW_CgkXfEEn9XJDDew==/com.phonepe.app-AKGkBUSK6asdKC3rzXw5Kg==/base.apk"
            },
            {
              "packageName": "com.whatsapp",
              "sourceDir": "/data/app/~~GuUU2tt9kUltlNHMcFOOcg==/com.whatsapp-jUzuqeqzYeB2vXvrjZDLPg==/base.apk"
            },
            {
              "packageName": "com.zupee.tc",
              "sourceDir": "/data/app/~~3GzJocn-NI-FlaZMUNvH3w==/com.zupee.tc-ZDLP21RDBmGFHvfNtIfL-A==/base.apk"
            },
            {
              "packageName": "com.ffgames.racingincar2",
              "sourceDir": "/data/app/~~9jzOUoxOle_xqqykKS_elQ==/com.ffgames.racingincar2-s8_sRHYj57-eVxaRlwYE2A==/base.apk"
            },
            {
              "packageName": "com.ansangha.drdriving",
              "sourceDir": "/data/app/~~aU6o7YH4GmuV2vYidlHoYg==/com.ansangha.drdriving-M2tlRiOnKugo00hldcIzLA==/base.apk"
            },
            {
              "packageName": "in.amazon.mShop.android.shopping",
              "sourceDir": "/data/app/~~aUamQUakmQovjhII3zbaqw==/in.amazon.mShop.android.shopping-gt7oQUvi4DnUw1U1DM30JQ==/base.apk"
            },
            {
              "packageName": "com.heallo.skinexpert",
              "sourceDir": "/data/app/~~TISgSyKk-8iEjXdLjcOEVQ==/com.heallo.skinexpert-btv3oouKvVeXlMDUyxXv3A==/base.apk"
            },
            {
              "packageName": "com.flipkart.android",
              "sourceDir": "/data/app/~~t37LfQFBm3Zu8AT0j_1elg==/com.flipkart.android-ExZpLbX4IxZxIW1XkQLlEQ==/base.apk"
            },
            {
              "packageName": "in.securegadgets.emiprotector",
              "sourceDir": "/data/app/~~i21YOwupJzsgjhts3Z2XCg==/in.securegadgets.emiprotector-d8lx99D38fCAuOpH9qvgQA==/base.apk"
            },
            {
              "packageName": "com.instagram.android",
              "sourceDir": "/data/app/~~awsFs0GRMYAG1pLt6mHwaQ==/com.instagram.android-yDSnpLPKBSUGwnWe7ilAfQ==/base.apk"
            },
            {
              "packageName": "com.olacabs.customer",
              "sourceDir": "/data/app/~~xzX-4iQBMWZ7-V9A3BeyrQ==/com.olacabs.customer-FgS8ZCYg2z7cVC6eifjLhA==/base.apk"
            },
            {
              "packageName": "com.pubg.imobile",
              "sourceDir": "/data/app/~~2IK_t2aHJtVLqX0uI87mkg==/com.pubg.imobile-EuRsdlGwepEpdEmv-6NBxg==/base.apk"
            },
            {
              "packageName": "com.mxtech.videoplayer.ad",
              "sourceDir": "/data/app/~~nabTrpxIW7WUzvH4QBsJUw==/com.mxtech.videoplayer.ad-3x3wKW1aNeg7IhXxO4yZNg==/base.apk"
            },
            {
              "packageName": "net.one97.paytm",
              "sourceDir": "/data/app/~~Vlqrf73tBu_0Mq3UNZdmDQ==/net.one97.paytm-Y6sEw6tDev8abONTtfK3vg==/base.apk"
            },
            {
              "packageName": "com.mediaplay.downloader",
              "sourceDir": "/data/app/~~184rsB9q5QSv0QcdKWGFiw==/com.mediaplay.downloader-TCNG-sq90IgrVp2X_ENhmg==/base.apk"
            },
            {
              "packageName": "com.jio.myjio",
              "sourceDir": "/data/app/~~SEOO2sHEyCmlEdBA1GdRPw==/com.jio.myjio-_mEnY-DnlcIvxHk-FGhFAg==/base.apk"
            },
            {
              "packageName": "com.ubercab",
              "sourceDir": "/data/app/~~AKrXaDgvGM31aonLlfb07w==/com.ubercab-Es1C3sRmoX3UC_w1ijXSKA==/base.apk"
            },
            {
              "packageName": "com.facebook.katana",
              "sourceDir": "/data/app/~~iFcm1Jvu0awIRqhrmihCgw==/com.facebook.katana-_fd269-NFxWGuxHinXG4AA==/base.apk"
            },
            {
              "packageName": "com.offshore.pikachu",
              "sourceDir": "/data/app/~~6p9ZIL48c-wazlrzUbbzKQ==/com.offshore.pikachu-OTQEy5x1xlqvCO9DeOrj8Q==/base.apk"
            },
            {
              "packageName": "com.dream11.fantasy.cricket.football.kabaddi",
              "sourceDir": "/data/app/~~d_fmPrTg2Poi7eFNmHDJKw==/com.dream11.fantasy.cricket.football.kabaddi-YfdPeAnAplflm7D-R_uNjQ==/base.apk"
            },
            {
              "packageName": "com.google.android.apps.translate",
              "sourceDir": "/data/app/~~bl6RBNFPTvXfgPt_-aieog==/com.google.android.apps.translate-47M2E_5ibt2YmstuKoHqeA==/base.apk"
            },
            {
              "packageName": "com.linkedin.android",
              "sourceDir": "/data/app/~~8W4dA2EYOR6ZkSGBYQqO9Q==/com.linkedin.android-0lJuf4nXHIBETsPo23D9HQ==/base.apk"
            },
            {
              "packageName": "com.grofers.customerapp",
              "sourceDir": "/data/app/~~elBpnLQyPWUO8p-ynxXYvA==/com.grofers.customerapp-LYhJRZWKcQp7U-AD6R9SVg==/base.apk"
            },
            {
              "packageName": "com.crrepa.band.dafit",
              "sourceDir": "/data/app/~~VV-6HR0aQzE4siMdL64wVw==/com.crrepa.band.dafit-CKxhm12dJmydNVRCYRDlnA==/base.apk"
            },
            {
              "packageName": "com.miniclip.cricketleague",
              "sourceDir": "/data/app/~~Jo-Q18ndVNeORUFoLayL7w==/com.miniclip.cricketleague--Sx5TC7Tdl8nAr43aaDMLg==/base.apk"
            },
            {
              "packageName": "com.snapmint.customerapp",
              "sourceDir": "/data/app/~~_NoPslRIBBgNU0_i828FgA==/com.snapmint.customerapp-_r9TGYlFWcXGESfnseZSRQ==/base.apk"
            },
            {
              "packageName": "com.ezt.pdfreader.pdfviewer",
              "sourceDir": "/data/app/~~YMhLTElptKzGqxtYHWcnyA==/com.ezt.pdfreader.pdfviewer-ubNltn16G_BxKGEoSkjFnA==/base.apk"
            },
            {
              "packageName": "com.vicman.toonmeapp",
              "sourceDir": "/data/app/~~roI_RsbKpMFpor-66mak-w==/com.vicman.toonmeapp-cZsPWS07XBnKfFkIIswXdQ==/base.apk"
            },
            {
              "packageName": "com.google.android.apps.youtube.music",
              "sourceDir": "/data/app/~~n-qWDE6pu_9FFp2_bRItyA==/com.google.android.apps.youtube.music-PhBAZPQgoRamcewwDxggNA==/base.apk"
            },
            {
              "packageName": "com.google.android.apps.adm",
              "sourceDir": "/data/app/~~c9n9xzdnBtviGEYD16rd7Q==/com.google.android.apps.adm-aS9uLaLxTYVK4zb5Y0rCig==/base.apk"
            },
            {
              "packageName": "com.y99.chat",
              "sourceDir": "/data/app/~~Pd4rya_l6oRYCG-Ua0R-Tg==/com.y99.chat-hG1ly7E5ujOv68Mm3S-jSw==/base.apk"
            },
            {
              "packageName": "com.gonext.gpsphotolocation",
              "sourceDir": "/data/app/~~l6yqFiuJoIEPPoXL01LcYQ==/com.gonext.gpsphotolocation-ua-2ZF6b8mX9MSxfjsPchw==/base.apk"
            },
            {
              "packageName": "com.snapchat.android",
              "sourceDir": "/data/app/~~oNmIMgcVFUOs1hEuLtnEHw==/com.snapchat.android-AWgAES9nhHcSZn4Kjg9e4g==/base.apk"
            },
            {
              "packageName": "com.meesho.supply",
              "sourceDir": "/data/app/~~192IF1SUnJXKyePlG8PMbw==/com.meesho.supply-xFZJLjGI7xs8Z06fosFi3w==/base.apk"
            }
          ],
          "appInstallTiming": "2024-01-08 18:45:07",
          "appUpdateTiming": "2025-06-09 09:23:14",
          "clickAutomatorInstalled": []
        },
        "osBuildRawData": {
          "fingerprint": "realme/RMX3785IN/RE5C6CL1:15/AP3A.240617.008/T.R4T2.1ddc98b_6788-f67f1:user/release-keys",
          "androidVersion": "15",
          "sdkVersion": "35",
          "kernelVersion": "5.15.149-android13-8-o-g3091e19198fc",
          "codecList": [],
          "encryptionStatus": "active_per_user",
          "securityProvidersData": [
            {
              "first": "AndroidNSSP",
              "second": "Android Network Security Policy Provider"
            },
            {
              "first": "AndroidOpenSSL",
              "second": "Android's OpenSSL-backed security provider"
            },
            {
              "first": "CertPathProvider",
              "second": "Provider of CertPathBuilder and CertPathVerifier"
            },
            {
              "first": "AndroidKeyStoreBCWorkaround",
              "second": "Android KeyStore security provider to work around Bouncy Castle"
            },
            {
              "first": "BC",
              "second": "BouncyCastle Security Provider v1.68"
            },
            {
              "first": "HarmonyJSSE",
              "second": "Harmony JSSE Provider"
            },
            {
              "first": "AndroidKeyStore",
              "second": "Android KeyStore security provider"
            }
          ],
          "kernelArch": "aarch64",
          "kernelName": "Linux"
        }
      },
      "networkData": {
        "networkLocation": {
          "latitude": 28.6061576,
          "longitude": 77.4290035
        }
      }
    },
    "userParams": {
      "phoneNumber": "7428947754",
      "userId": "13369818",
      "phoneInputType": "GOOGLE_HINT",
      "otpInputType": "AUTO_FILLED",
      "userEventType": "LOGIN",
      "sessionId": "edc24083-2298-4473-afe5-2b9d2a642ef2",
      "additionalParams": {},
      "source": "G"
    },
    "clientSessionId": "edc24083-2298-4473-afe5-2b9d2a642ef2",
    "userId": "13369818",
    "phoneNumber": "7428947754"
  },
  "response": {
    "requestId": "d96b1aae-48e8-4f91-acac-61750a8f2fdf",
    "createdAtInMillis": 1749630649645,
    "ipIntelligenceData": {
      "ip": "103.179.8.12"
    },
    "androidIntelligenceData": {
      "isRemoteControlProvider": "false",
      "remoteAppProvidersCount": 0,
      "isScreenBeingMirrored": "false",
      "isVpn": "false",
      "isProxy": "false",
      "isEmulator": "false",
      "isAppCloned": "false",
      "isGeoSpoofed": "false",
      "isRooted": "false",
      "isHooking": "false",
      "isFactoryReset": "false",
      "isAppTampering": "false",
      "gpsLocation": {}
    },
    "fingerPrintData": {
      "deviceId": "76ad30f2-f26e-4351-b87c-9b7f5d422778",
      "deviceIdentifiers": {
        "androidId": "d9950404fdd75a32",
        "adId": "593fd5ac-a477-48b1-98df-89aa4d176366",
        "gsfId": "",
        "drmId": "1b03c01ce99c35ab821862fd8ad81197abbaf636c8ab92f20c06be8aa421a3e1",
        "fingerPrintHash": {
          "hardwareFingerprint": "31c43f9d30e338da560eea63842f38a8",
          "osBuildFingerprint": "7a64329545f035921fe01759c09cd1bc",
          "deviceStateFingerprint": "8171f3fe6fe1d94fbeff13a6cfece579",
          "deviceFingerprint": "21975f96f3cce96c455ec17f49fffb5b"
        }
      },
      "newDevice": "false",
      "esId": "VklUmJIBeMEpcrh3lZia",
      "mt": "s:i",
      "identifiersChanged": "false",
      "error": "false"
    },
    "sessionRiskScore": 0.0,
    "deviceRiskScore": 0.0,
    "aggregatedVpn": "false",
    "aggrgatedProxy": "false",
    "clientUserIds": [
      "13369818"
    ],
    "sign3UserIds": [],
    "cardDetails": [],
    "simInfo": {
      "simIds": []
    },
    "deviceMeta": {
      "cpuType": "Unknown",
      "product": "RMX3785IN",
      "androidVersion": "15",
      "storageAvailable": "48385368064",
      "storageTotal": "115911655424",
      "model": "RMX3785",
      "screenResolution": "1080x2290",
      "brand": "realme",
      "totalRAM": "8003399680"
    }
  },
  "timestamp": 1749630649,
  "eventName": "user_insights_v3",
  "partitionId": "76ad30f2-f26e-4351-b87c-9b7f5d422778",
  "sinkPartitionId": "116",
  "originalSha1": "86:a3:58:5c:eb:d5:91:d7:27:d9:2b:f8:61:4b:5b:51:db:c3:73:7c",
  "originalPackageName": "com.snapmint.customerapp",
  "namespace": "jarvis",
  "jarvisEnv": "prod",
  "partitionKey": "76ad30f2-f26e-4351-b87c-9b7f5d422778"
}
    
prepare_query(record)




