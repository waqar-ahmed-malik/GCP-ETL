import os


'''
# Directory Info

os.getcwd()                             # get current working directory.
os.chdir("C:\\Users\\User\\Desktop")    # change current working directory.
os.getcwd()                             # now will print the changed cwd.
os.listdir()                            # list folders in the cwd or the directory that has been passed as an arg.
'''


'''
# Create a Directory

os.mkdir()                              # create a new directory without any sub-folders.
os.makedirs()                           # create a new directory even with sub-folders that doesn't exist before.
'''


'''
# Remove a Directory.

os.rmdir()                            # remove directory.
os.removedirs()                       # remove directories with sub folders so should not be used.
'''


'''
# Rename a file

os.rename("original.txt", "rename.txt")     # rename a file.
os.stat("rename.txt")                       # get stats of file like size, created time or modified time.
'''


'''
# traverse a directory from top to bottom with every detail.

for dirpath, dirnames, filenames in os.walk(os.getcwd()):
    print("Current Path :", dirpath )                           # directory path
    print("Directories :", dirnames)                            # Folders in that directory
    print("Files :", filenames)                                 # Files in that directory.
    print()
'''


'''
# Environment Variable Info
 
os.environ                       # return all the environment variables and there value
os.environ.get('PATH')           # returns the value of PATH environment variable.
'''


'''
# Join Path with a path or a file name.

path1 = os.environ.get('PATH')
path2 = 'file.txt'

os.path.join(path1, path2)          # always join two paths or filename with a path using this as there can be
                                    # problem with \\ or which \/ to use or whether path one has extra / append to it.
'''


'''
# Extract Values from a path

os.path.basename("/tmp/test.txt")   # returns filename even if the path doesn't exist.
os.path.dirname("/tmp/test.txt")    # returns directory name even if the path doesn't exist.
os.path.split("/tmp/test.txt")      # returns both even if both don't exist.
os.path.exists("/tmp/test.txt")     # returns true or false based on if the path exists or not.
os.path.isdir()                     # checks if the path a directory.
os.path.isfile()                    # checks if the path is a file.
os.path.splitext("/tmp/test.txt")   # slits path and file extension.
'''

