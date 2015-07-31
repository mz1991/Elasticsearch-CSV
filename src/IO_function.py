import csv

def get_CSV_rows(CSV_path,CSV_delimiter,CSV_quotechar):
	with open(CSV_path,'r') as csvfile:
		return list(csv.reader(csvfile,delimiter=CSV_delimiter,quotechar=CSV_quotechar))
