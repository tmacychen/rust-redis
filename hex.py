hex_data = "524544495330303131fa0972656469732d76657205372e322eg30fa0a72656469732d62697473c040fe00fb010000056d616e676f0970696e656170706c65ff0c1bcb91b2edf619"

# 去除可能存在的空格并转换为字节
hex_data = hex_data.replace(" ", "")
binary_data = bytes.fromhex(hex_data)

# 写入文件
with open("dump.rdb", "wb") as file:
    file.write(binary_data)

print("文件已成功写入为 output.dat")
