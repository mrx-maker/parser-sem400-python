import datetime
import random
import string
import psycopg2

import paho.mqtt.client as mqtt
from psycopg2._psycopg import cursor

from data_decoder import DecodeDLMS

MQTT_Broker = "59.175.214.11"
MQTT_Port = 8091
Keep_Alive_Interval = 60
MQTT_Topic = "app_bin/route/102504020001/rxcmd"

DB_Schema = "postgres"
DB_User = "postgres"
DB_Password = "Solo123."
DB_Host = "localhost"
DB_Port = "5432"

# Prepare the DB Connection
conn = psycopg2.connect(
    dbname=DB_Schema,
    user=DB_User,
    password=DB_Password,
    host=DB_Host,
    port=DB_Port
)

def ensure_connection():
    global conn
    if conn.closed != 0:
        conn = psycopg2.connect(
            dbname=DB_Schema,
            user=DB_User,
            password=DB_Password,
            host=DB_Host,
            port=DB_Port
        )
def execute_payload(payload: bytearray) -> bool:
    result = False

    try:
        # Check start and end char
        if payload[0] == 0x68 and payload[-1] == 0x16:
            length = (payload[2] << 8) | payload[1]

            # Check full length: 4 = start + length(2) + end
            if length == len(payload):
                checksum = sum(payload[3:-2]) & 0xFF  # sum of data area

                if checksum == payload[-2]:
                    control_field = payload[3]
                    R = ''.join(chr(payload[4 + i]) for i in range(5))
                    seq_number = payload[9]
                    AFN = payload[10]

                    if AFN == 0xF0:
                        meter_serial = ''.join(f'{payload[24 - i]:02x}' for i in range(6))

                        obis_code = [payload[30 - i] for i in range(6)]
                        attr_id = payload[31]
                        status = payload[32]
                        data_len = (payload[34] << 8) | payload[33]
                        #data = ''.join(chr(payload[35 + i]) for i in range(data_len))
                        data = bytearray(payload[35 : 35 + data_len])

                        if obis_code == [1, 0, 99, 1, 0, 255]:
                            #load profile
                            cos = DecodeDLMS(data)
                            if cos.DataArray() and len(cos.DataArray()[0].DataStructure()) == 16:
                                s = cos.DataArray()[0].DataStructure()
                                meter_time = s[0].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S")

                                sql_del = """
                                    DELETE FROM loadprofile
                                    WHERE MeterSerialNumber = %s AND LPTime = %s
                                    """

                                sql_ins = """
                                    INSERT INTO loadprofile (
                                        MeterSerialNumber, ReadingTime, LPTime, ActPlus, ActMin, ReactPlus, ReactMin, 
                                        V1, I1, PowerFactor, 
                                        EnergyMonthly, AR, Status, ActPwrPlus, ActPwrMin, Frequency) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s)
                                        """
                                ins_data = (
                                    meter_serial,
                                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    meter_time,
                                    s[8].DataDoubleLongUnsigned(),
                                    s[9].DataDoubleLongUnsigned(),
                                    s[10].DataDoubleLongUnsigned(),
                                    s[11].DataDoubleLongUnsigned(),
                                    s[3].DataDoubleLong()/10,
                                    s[4].DataDoubleLong()/1000,
                                    s[5].DataDoubleLong()/1000,
                                    s[12].DataDoubleLongUnsigned(),
                                    s[13].DataBitString().raw,
                                    int(s[2].DataBoolean()),
                                    s[6].DataDoubleLongUnsigned(),
                                    s[7].DataDoubleLongUnsigned(),
                                    s[14].DataLongUnsigned() / 100,
                                )

                                del_data = (
                                    meter_serial,
                                    meter_time
                                )

                                ensure_connection()
                                with conn.cursor() as cur:
                                    cur.execute(sql_del, del_data)
                                    cur.execute(sql_ins, ins_data)
                                    conn.commit()

                                result = True
                        elif obis_code == [1,96,98,128,0,255]:
                            #instant profile
                            cos = DecodeDLMS(data)
                            if cos.DataArray() and len(cos.DataArray()[0].DataStructure()) == 14:
                                s = cos.DataArray()[0].DataStructure()
                                meter_time = s[0].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S")

                                sql_del = """
                                          DELETE \
                                          FROM instantaneous
                                          WHERE MeterSerialNumber = %s \
                                            AND meterdate = %s \
                                          """

                                sql_ins = """
                                          INSERT INTO instantaneous (MeterSerialNumber, readingdate, meterdate, VOLTAGE1, CURRENT1, CURRENTNet, CosPhi1, CosPhi, 
                                                                     AngleVL1, AngleVI_L1, ActPlusPwr, ActMinPwr, RectPlusPwr, RectMinPwr, AppPlusPwr, AppMinPwr, 
                                                                     ActivePlus, ActiveMin, REACTIVEPLUS, ReactiveMin, ReactiveBilling, Frequency, 
                                                                     CURRENTTHD, CURRENTTDD, Battery) \
                                          VALUES (  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                    %s, %s, %s, %s, %s) \
                                          """
                                ins_data = (
                                    meter_serial,
                                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    meter_time,
                                    s[2].DataDoubleLong()/10,
                                    s[3].DataDoubleLong()/1000,
                                    s[4].DataDoubleLong()/1000,
                                    s[5].DataDoubleLong()/1000,
                                    s[5].DataDoubleLong()/1000,

                                    s[6].DataDoubleLong(),
                                    s[7].DataDoubleLong(),

                                    s[8].DataDoubleLongUnsigned(),
                                    s[9].DataDoubleLongUnsigned(),
                                    s[10].DataDoubleLongUnsigned(),
                                    s[11].DataDoubleLongUnsigned(),
                                    s[12].DataDoubleLongUnsigned(),
                                    s[13].DataDoubleLongUnsigned(),

                                    s[14].DataDoubleLongUnsigned(),
                                    s[15].DataDoubleLongUnsigned(),
                                    s[16].DataDoubleLongUnsigned(),
                                    s[17].DataDoubleLongUnsigned(),
                                    s[18].DataDoubleLongUnsigned(),

                                    s[19].DataLongUnsigned()/100,
                                    s[20].DataLongUnsigned()/10,
                                    s[21].DataLongUnsigned()/10,
                                    s[23].DataLongUnsigned()/1000
                                )

                                del_data = (
                                    meter_serial,
                                    meter_time
                                )

                                ensure_connection()
                                with conn.cursor() as cur:
                                    cur.execute(sql_del, del_data)
                                    cur.execute(sql_ins, ins_data)
                                    conn.commit()

                                result = True
                        elif obis_code == [0,0,98,1,0,255]:
                            #end of billing
                            cos = DecodeDLMS(data)
                            if cos.DataArray() and len(cos.DataArray()[0].DataStructure()) == 54:
                                s = cos.DataArray()[0].DataStructure()
                                meter_time = s[0].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S")

                                sql_del = """
                                          DELETE \
                                          FROM billingregister
                                          WHERE MeterSerialNumber = %s \
                                            AND BillingDate = %s \
                                          """

                                sql_ins = """
                                          INSERT INTO billingregister (MeterSerialNumber, ReadingDate, BillingDate, 
                                                                       ActivePlusT1, ActivePlusT2, ActivePlusT3, ActivePlusT4, ActivePlusT5, ActivePlus, 
                                                                       ActiveMinusT1, ActiveMinusT2, ActiveMinusT3, ActiveMinusT4, ActiveMinusT5, ActiveMinus, 
                                                                       ReactivePlusT1, ReactivePlusT2, ReactivePlusT3, ReactivePlusT4, ReactivePlusT5, ReactivePlus, 
                                                                       ReactiveMinusT1, ReactiveMinusT2, ReactiveMinusT3, ReactiveMinusT4, ReactiveMinusT5, ReactiveMinus, 
                                                                       ApparentPlusT1, ApparentPlusT2, ApparentPlusT3, ApparentPlusT4, ApparentPlusT5, ApparentPlus, 
                                                                       ApparentMinusT1, ApparentMinusT2, ApparentMinusT3, ApparentMinusT4, ApparentMinusT5, ApparentMinus, 
                                                                       ReactivePlusBilling, 
                                                                       MD,  MD_Time, MDT1, MDT1_Time, 
                                                                       MDT2, MDT2_Time, MDT3, MDT3_Time, 
                                                                       MDT4, MDT4_Time, MDT5, MDT5_Time, 
                                                                       NumberPwrOff, NumberTamper, Battery) \
                                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                  %s, %s, %s, %s, %s) \
                                          """
                                ins_data = (
                                    meter_serial,
                                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    meter_time,
                                    s[2].DataDoubleLongUnsigned(),
                                    s[3].DataDoubleLongUnsigned(),
                                    s[4].DataDoubleLongUnsigned(),
                                    s[5].DataDoubleLongUnsigned(),
                                    s[6].DataDoubleLongUnsigned(),
                                    s[7].DataDoubleLongUnsigned(),
                                    s[8].DataDoubleLongUnsigned(),
                                    s[9].DataDoubleLongUnsigned(),
                                    s[10].DataDoubleLongUnsigned(),
                                    s[11].DataDoubleLongUnsigned(),
                                    s[12].DataDoubleLongUnsigned(),
                                    s[13].DataDoubleLongUnsigned(),

                                    s[14].DataDoubleLongUnsigned(),
                                    s[15].DataDoubleLongUnsigned(),
                                    s[16].DataDoubleLongUnsigned(),
                                    s[17].DataDoubleLongUnsigned(),
                                    s[18].DataDoubleLongUnsigned(),
                                    s[19].DataDoubleLongUnsigned(),
                                    s[20].DataDoubleLongUnsigned(),
                                    s[21].DataDoubleLongUnsigned(),
                                    s[22].DataDoubleLongUnsigned(),
                                    s[23].DataDoubleLongUnsigned(),
                                    s[24].DataDoubleLongUnsigned(),
                                    s[25].DataDoubleLongUnsigned(),

                                    s[26].DataDoubleLongUnsigned(),
                                    s[27].DataDoubleLongUnsigned(),
                                    s[28].DataDoubleLongUnsigned(),
                                    s[29].DataDoubleLongUnsigned(),
                                    s[30].DataDoubleLongUnsigned(),
                                    s[31].DataDoubleLongUnsigned(),
                                    s[32].DataDoubleLongUnsigned(),
                                    s[33].DataDoubleLongUnsigned(),
                                    s[34].DataDoubleLongUnsigned(),
                                    s[35].DataDoubleLongUnsigned(),
                                    s[36].DataDoubleLongUnsigned(),
                                    s[37].DataDoubleLongUnsigned(),

                                    s[38].DataDoubleLongUnsigned(),

                                    s[39].DataDoubleLongUnsigned(),
                                    s[40].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S"),
                                    s[41].DataDoubleLongUnsigned(),
                                    s[42].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S"),
                                    s[43].DataDoubleLongUnsigned(),
                                    s[44].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S"),
                                    s[45].DataDoubleLongUnsigned(),
                                    s[46].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S"),
                                    s[47].DataDoubleLongUnsigned(),
                                    s[48].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S"),
                                    s[49].DataDoubleLongUnsigned(),
                                    s[50].DataOctetString().tgl.strftime("%Y-%m-%d %H:%M:%S"),

                                    s[51].DataDoubleLongUnsigned(),
                                    s[52].DataDoubleLongUnsigned(),
                                    s[53].DataLongUnsigned()
                                )

                                del_data = (
                                    meter_serial,
                                    meter_time
                                )

                                ensure_connection()
                                with conn.cursor() as cur:
                                    cur.execute(sql_del, del_data)
                                    cur.execute(sql_ins, ins_data)
                                    conn.commit()

                                result = True

                    elif AFN == 0x06:
                        device_type = payload[13]
                        if device_type == 0x05:
                            protocol_type = payload[14]
                            data_len = payload[15]
                            data_enum = payload[16]
                            meter_serial = ''.join(f'{payload[22 - i]:02x}' for i in range(6))
                            print(f"{meter_serial}\t{data_enum}")
                    else:
                        raise Exception("AFN is not 0xF0!")
                else:
                    raise Exception("Checksum is not valid!")
            else:
                raise Exception("Length is not valid!")
        else:
            raise Exception("Start and end char is not valid!")
    except Exception as e:
        print("[ERROR]", str(e))
        result = False
    finally:
        print("Exit execute_payload()")

    return result

def generate_client_id(length=16):
    chars = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
    client_id = ''.join(random.choice(chars) for _ in range(length))
    return client_id

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, reasonCode, properties):
    print(f"Connected with reason code {reasonCode}")
    # Subscribe to a topic once connected
    client.subscribe(MQTT_Topic)

# Callback when a PUBLISH message is received from the server
def on_message(client, userdata, msg):
    print(f"Received message on topic {msg.topic}: {msg.payload}")
    execute_payload(bytearray(msg.payload))

def main():

    cursor = conn.cursor()

    # Create MQTT client with the latest callback API
    random_client_id = generate_client_id()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id = random_client_id)
    # Attach callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_Broker, MQTT_Port, Keep_Alive_Interval)
    client.loop_forever()

if __name__ == "__main__":
    main()

