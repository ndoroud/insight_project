# Import the list of NERL files
#
with open("project_directory.txt","r") as pdir:
    project_folder = pdir.read().strip()
    
with open(project_folder+"/lists/nrel_filelist.csv","r") as file:
    file_list=[]
    for line in file:
        file_list.append(project_folder+"/data/nrel/"+line.strip()+"TYA.CSV")
#
#
#
# Extract the headers from data files and store them in a dictionary with states as keys
#
headers={"state" : [["file_id","location","state","time_zone","latitude","longitude","elevation"]]}
for entry in file_list:
    with open(entry,"r") as file:
        y = 0
        for line in file:
            line_data = line.strip().split(",")
            if line_data[2] in headers.keys():
                pass
            else:
                headers[line_data[2]]=[]
            if y<1:
                headers[line_data[2]].append(line_data)
                y += 1
            else:
                break
#
#
#
#
with open(project_folder+"/lists/nrel_headers.csv","w") as headers_file:
    for state in headers.keys():
        for entry in headers[state]:
            headers_file.write(",".join(entry)+"\n")
    