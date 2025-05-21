import datetime
from enum import Enum

class IMPLICIT(Enum):
    xxxNull = 0
    xxxArray = 1
    xxxStructure = 2
    xxxBoolean = 3
    xxxBitstring = 4
    xxxDoubleLong = 5
    xxxDoubleLongUnsigned = 6
    xxxFloating = 7
    xxxOctetString = 9
    xxxVisibleString = 10
    xxxUTF8 = 12
    xxxBCD = 13
    xxxInteger = 15
    xxxLong = 16
    xxxUnsigned = 17
    xxxLongUnsigned = 18
    xxxLong64 = 20
    xxxLong64Unsigned = 21
    xxxEnum = 22

class BitString:
    raw = ""
    length = 0
    def __init__(self, data):
        self.raw = data
        self.length = len(data)

class OctetString:
    bytearr = []
    datevalid = False
    logicalnamevalid = False
    tgl = datetime.datetime.min
    logicalname = ""
    def __init__(self, data):
        self.bytearray = data
        if len(data) == 12:
            self.datevalid = True
            try:
                self.tgl = datetime.datetime((data[0] << 8) + data[1], data[2], data[3], data[5], data[6],data[7], 0)
            except:
                self.datevalid = False
        elif len(data) == 6:
            self.logicalnamevalid = True
            self.logicalname = str(data[0]) + "-" + str(data[1]) + ":" + str(
                data[2]) + "." + str(data[3]) + "." + str(data[4]) + "." + str(
                data[5])

class VisibleString:
    bytearr = []
    tgl = datetime.datetime.min
    logicalname = ""
    def __init__(self, data):
        self.bytearray = data

class BCD:
    bytearr = []
    def __init__(self, data):
        self.bytearray = data

class CosData:
    type = IMPLICIT.xxxNull
    data = object

    def __init__(self, type, cd):
        self.type = type
        self.data = cd

    def DataNull(self):
        return None

    def DataArray(self):
        return self.data

    def DataStructure(self):
        return self.data

    def DataEnum(self):

        return self.data
    def DataDoubleLongUnsigned(self):
        return self.data

    def DataDoubleLong(self):
        return self.data

    def DataLongUnsigned(self):
        return self.data

    def DataLong(self):
        return self.data

    def DataUnsigned(self):
        return self.data

    def DataInteger(self):
        return self.data

    def DataLong64Unsigned(self):
        return self.data

    def DataLong64(self):
        return self.data

    def DataOctetString(self):
        return self.data

    def DataVisibleString(self):
        return self.data

    def DataBitString(self):
        return self.data

    def DataBoolean(self):
        return self.data

def DecodeArray(buff):
    tag = buff[1]
    if tag == 0x82:
        length = buff[2] << 8
        length += buff[3]
        del buff[0:4]
    elif tag == 0x81:
        length = buff[2]
        del buff[0:3]
    else:
        length = buff[1]
        del buff[0:2]
    cos = CosData(IMPLICIT.xxxArray, [])
    for x in range(length):
        cos.DataArray().append(DecodeData(buff))
    return cos

def DecodeStructure(buff):
    tag = buff[1]
    if tag == 0x82:
        length = buff[2] << 8
        length += buff[3]
        del buff[0:4]
    elif tag == 0x81:
        length = buff[2]
        del buff[0:3]
    else:
        length = buff[1]
        del buff[0:2]

    cos = CosData(IMPLICIT.xxxStructure, [])
    for x in range(length):
        cos.DataStructure().append(DecodeData(buff))
    return cos

def DecodeOctetString(buff):
    length = buff[1]
    del buff[0:2]
    octet = OctetString(buff[0:length])

    cos = CosData(IMPLICIT.xxxOctetString, octet)
    del buff[0:length]

    return cos

def DecodeBitString(buff):
    length = buff[1]
    del buff[0:2]
    bitLen = (length + 8 - 1) // 8
    str_value = buff[0:bitLen]

    str_bin = ""
    for ch in str_value:
        str_bin += format(ch, '08b')

    bitstring = BitString(str_bin)

    cos = CosData(IMPLICIT.xxxBitstring, bitstring)
    del buff[0:bitLen]

    return cos

def DecodeVisibleString(buff):
    length = buff[1]
    del buff[0:2]
    visible = VisibleString(buff[0:length])

    cos = CosData(IMPLICIT.xxxVisibleString, visible)
    del buff[0:length]

    return cos

def DecodeBCD(buff):
    length = buff[1]
    del buff[0:2]
    bcd = BCD(buff[0:length])

    cos = CosData(IMPLICIT.xxxBCD, bcd)
    del buff[0:length]

    return cos

def DecodeDoubleLongUnsigned(buff):
    temp = []
    for x in range(4):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=False)
    cos = CosData(IMPLICIT.xxxDoubleLongUnsigned, val)
    del buff[0:5]
    return cos

def DecodeDoubleLong(buff):
    temp = []
    for x in range(4):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=True)
    cos = CosData(IMPLICIT.xxxDoubleLong, val)
    del buff[0:5]
    return cos

def DecodeLongUnsigned(buff):
    temp = []
    for x in range(2):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=False)
    cos = CosData(IMPLICIT.xxxLongUnsigned, val)
    del buff[0:3]
    return cos

def DecodeLong(buff):
    temp = []
    for x in range(2):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=True)
    cos = CosData(IMPLICIT.xxxLong, val)
    del buff[0:3]
    return cos

def DecodeLong64Unsigned(buff):
    temp = []
    for x in range(8):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=False)
    cos = CosData(IMPLICIT.xxxLong64Unsigned, val)
    del buff[0:9]
    return cos

def DecodeLong64(buff):
    temp = []
    for x in range(8):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=True)
    cos = CosData(IMPLICIT.xxxLong64, val)
    del buff[0:9]
    return cos

def DecodeUnsigned(buff):
    temp = []
    for x in range(1):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=False)
    cos = CosData(IMPLICIT.xxxUnsigned, val)
    del buff[0:2]
    return cos

def DecodeInteger(buff):
    temp = []
    for x in range(1):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=False)
    cos = CosData(IMPLICIT.xxxInteger, val)
    del buff[0:2]
    return cos

def DecodeEnum(buff):
    temp = []
    for x in range(1):
        temp.append(buff[1+x])
    val=int.from_bytes(temp, "big", signed=True)
    cos = CosData(IMPLICIT.xxxEnum, val)
    del buff[0:2]
    return cos

def DecodeBoolean(buff):
    val = True
    if buff[1] == 0:
        val = False
    cos = CosData(IMPLICIT.xxxEnum, val)
    del buff[0:2]
    return cos

def DecodeData(buff):
    if buff[0] == IMPLICIT.xxxArray.value:
        return DecodeArray(buff)
    if buff[0] == IMPLICIT.xxxStructure.value:
        return DecodeStructure(buff)
    if buff[0] == IMPLICIT.xxxEnum.value:
        return DecodeEnum(buff)
    elif buff[0] == IMPLICIT.xxxOctetString.value:
        return DecodeOctetString(buff)
    elif buff[0] == IMPLICIT.xxxVisibleString.value:
        return DecodeVisibleString(buff)
    elif buff[0] == IMPLICIT.xxxBCD.value:
        return DecodeBCD(buff)
    elif buff[0] == IMPLICIT.xxxBitstring.value:
        return DecodeBitString(buff)
    elif buff[0] == IMPLICIT.xxxDoubleLongUnsigned.value:
        return DecodeDoubleLongUnsigned(buff)
    elif buff[0] == IMPLICIT.xxxDoubleLong.value:
        return DecodeDoubleLong(buff)
    elif buff[0] == IMPLICIT.xxxLongUnsigned.value:
        return DecodeLongUnsigned(buff)
    elif buff[0] == IMPLICIT.xxxLong.value:
        return DecodeLong(buff)
    elif buff[0] == IMPLICIT.xxxLong64Unsigned.value:
        return DecodeLong64Unsigned(buff)
    elif buff[0] == IMPLICIT.xxxLong64.value:
        return DecodeLong64(buff)
    elif buff[0] == IMPLICIT.xxxUnsigned.value:
        return DecodeUnsigned(buff)
    elif buff[0] == IMPLICIT.xxxInteger.value:
        return DecodeInteger(buff)
    elif buff[0] == IMPLICIT.xxxBoolean.value:
        return DecodeBoolean(buff)
    else:
        return CosData(IMPLICIT.xxxNull, None)

def DecodeDLMS(buff):
    return DecodeData(buff)

def PrintArray(cos):
    length = len(cos.DataArray())
    value = "Array (" + str(length) + "):\n";
    for x in range(length):
        value += PrintDetail(cos.DataArray()[x])
    return value

def PrintStructure(cos):
    length = len(cos.DataStructure())
    value = "Structure (" + str(length) + "):\n";
    for x in range(length):
        value += PrintDetail(cos.DataStructure()[x])

    return value

def PrintOctetString(cos):
    length = len(cos.DataOctetString())
    value = "OctetString:\n"
    value += cos.DataOctetString() + "\n"

    return value

def PrintVisibleString(cos):
    length = len(cos.DataVisibleString())
    value = "OctetString:\n"
    value += cos.DataVisibleString() + "\n"

    return value

def PrintDetail(cos):
    if cos.type == IMPLICIT.xxxArray:
        return PrintArray(cos)
    if cos.type == IMPLICIT.xxxStructure:
        return PrintStructure(cos)
    elif cos.type == IMPLICIT.xxxOctetString:
        return PrintOctetString(cos)
    elif cos.type == IMPLICIT.xxxVisibleString:
        return PrintVisibleString(cos)
    # elif cos.type == IMPLICIT.xxxDoubleLongUnsigned:
    #     return PrintDoubleLongUnsigned(cos)
    # elif cos.type == IMPLICIT.xxxDoubleLong:
    #     return PrintDoubleLong(cos)
    # elif cos.type == IMPLICIT.xxxLongUnsigned:
    #     return PrintLongUnsigned(cos)
    # elif cos.type == IMPLICIT.xxxLong:
    #     return PrintLong(cos)
    # elif cos.type == IMPLICIT.xxxLong64Unsigned:
    #     return PrintLong64Unsigned(cos)
    # elif cos.type == IMPLICIT.xxxLong64:
    #     return PrintLong64(cos)
    # elif cos.type == IMPLICIT.xxxUnsigned:
    #     return PrintUnsigned(cos)
    # elif cos.type == IMPLICIT.xxxInteger:
    #     return PrintInteger(cos)
    else:
        return "Don't Care"

def PrintCosem(cos):
    return PrintDetail(cos)
