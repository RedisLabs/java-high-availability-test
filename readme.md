# High availability and consistency test code 

simple application that will constantly run and check if data is consistent. it uses redis increment
method in order to both read and write in the single operation. then it matchs the value with the local copy.

while running this program shutdown/restart redis process/proxy process/servers and check how to client recover and 
if data remain consistent 