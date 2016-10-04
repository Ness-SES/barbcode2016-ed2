import random

with open("pagerank.txt", "a") as myfile:
    num_pages = 100001
    line = ''
    for i in range(1,num_pages):
        line += str(i)
        num_links = random.randrange(num_pages)
        for j in range(0,num_links):
            while True:
                link_page = random.randrange(num_pages)
                if link_page != i:
                    break
            line += ',' + str(link_page)
        line += '\n'
        if i % 10000 == 0:  
    	    myfile.write(line)
            line = ''


