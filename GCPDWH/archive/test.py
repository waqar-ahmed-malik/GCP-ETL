col_specification =[0,7,9,12,15,42,45,49,69,89,104,107,108,109,119,120,121,122,131,151,155,157,159,181,191,284,285,287,289,293,303,367,368,369,370]
print(len(col_specification))
# [0,7,9,12,-15,42,45,49,69,89,104,107,-108,109,119,-120,121,-122
# ,131,151,-155,
# 157,-159,
# 181,-191,
# 284,-285
# ,287,-289,293,-303,367,-368,369,-370]

# [0,7,9,12,42,45,49,69,89,104,107,109,119,121,131,151,157,181,284,287,293,367,369]
# [0,7,9,12,42,45,49,69,89,104,107,109,119,121,131,151,157,181,284,287,293,367, 369]

col_specification.pop(4)
col_specification.pop(11)
col_specification.pop(13)
col_specification.pop(14)
col_specification.pop(16)
col_specification.pop(17)
col_specification.pop(18)
col_specification.pop(19)
col_specification.pop(20)
col_specification.pop(21)
col_specification.pop(22)
col_specification.pop()
#del col_specification[1]

print(col_specification)       