
import constant_name

def create_bulk_data(csv_lines,csv_header,index_name,type_name):
   bulk_data = [] 
   id_index=1
   del csv_lines[0]
   for row in csv_lines:
      data_dict = {}
      for i in range(len(row)):
          data_dict[csv_header[i]] = row[i]
      op_dict = {
          "index": {
            "_index": index_name, 
            "_type": type_name, 
            "_id": id_index
          }
      }
      bulk_data.append(op_dict)
      bulk_data.append(data_dict)
      id_index=id_index+1
   return bulk_data
