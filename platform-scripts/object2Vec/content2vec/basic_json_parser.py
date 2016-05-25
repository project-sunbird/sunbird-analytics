def extract_json_data(json_file_names):
#Parse json files
    json_files=[]
    for i in json_file_names:
        with open(i,encoding="utf8") as f:
            try:
                json_data = json.load(f)
                json_files.append(get_all_values(json_data))
            except ValueError:
                print("Error in",i)
    if(len(json_files)>0):
        return json_files
    else:
        print("Empty json files")
        return