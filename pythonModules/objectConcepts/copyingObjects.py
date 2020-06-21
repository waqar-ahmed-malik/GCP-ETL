"""
Copying Behavior Of Objects in Python

Run The Following Code and Observe the output

-----------------------------------------------------------------------------------------------------------------------

a = 2
b = a
c = 'Waqar'
d = c
e = [1, 2]
f = e
g = {'key': 'old value'}
h = g
print('Old Values: ', 'a: ', a, 'b: ', b, 'c: ', c, 'd: ', d, 'e: ', e, 'f: ', f, 'g: ', g, 'h: ', h)


b = b + 1
d = '{} {}'.format(d, 'Ahmed')
f.append(3)
f[0] = 0
h['key'] = 'new_value'
print('New Values: ', 'a: ', a, 'b: ', b, 'c: ', c, 'd: ', d, 'e: ', e, 'f: ', f, 'g: ', g, 'h: ', h)

-----------------------------------------------------------------------------------------------------------------------

Observations:

1.  After copying string and integer objects, if we change the value of copied object the value of the original object 
    doesn't change. Immutable Objects.

2.  After copying list and dict objects, if we change the value of copied object the value of the original object 
    also change. Mutable Objects.

Mutable and Call by Reference (deep copy)
Immutable and Call by Value (normal copy)


Best Practice During Copy - copy()
Run the below code and observe the behavior

a = [1, 2, 3]
b = a
c = a.copy()
b.append(4)
c.append(5)
print('a: ', a)
print('b: ', b)
print('c: ', c)

Observation:

If we change value of a or b then both changes. But change in value of c doesn't effect a.
In case of Mutable objects when we assign an object to another object then same memory address is assigned to both the 
Objects.

So we should use copy() to create a separate object altogether.
"""