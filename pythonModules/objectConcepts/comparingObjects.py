"""
Difference between "==" and "is"

a = [1, 2, 3]
b = [1, 2, 3]
print(id(a))
print(id(b))
print(a == b)   # compare values in the lists               returns true
print(a is b)   # compare ids in the memory for a and b     returns false as evaluates to id(a) == id(b) and since they
# are two different objects then ids are also different.

a = b   # Now both objects point to the same location so
# Both below statements return true
print(a == b)   # compare values in the lists               returns true
print(a is b)   # compare ids in the memory for a and b     returns true

"""