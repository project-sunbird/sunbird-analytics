# Author: Aditya Arora, adityaarora@ekstepplus.org

#Get all unique values corresponding to all keys in a dictionary(including any enclosed sub dictionaries)
def getAllValues(obj):
	values=[]
	if(isinstance(obj,dict)):
		keys=obj.keys()
		for key in keys:
			val_list=getAllValues(obj[key])
			for value in val_list:
				if value not in values:
					values.append(value)
	elif(isinstance(obj,list)):
		for dct in obj:
			val_list=getAllValues(dct)
			for value in val_list:
				if value not in values:
					values.append(value)
	else:
		values=[obj]
	return values
