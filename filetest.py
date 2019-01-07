
name = 'test'
# open the master file for this thread & read its contents into memory
filename = name + '.txt'
with open(filename, 'r') as file: filedata = file.read()


data = ['Thomas', 'Richard', "Curie is a poops\nShe's such a poops", 'Laura', 'Jennifer']

# insert any new posts
for d in data: filedata = filedata.replace('</ol>', d + '\n</ol>')

# write the output
with open(filename, 'w') as file: file.write(filedata)
