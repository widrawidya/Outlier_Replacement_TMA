#Anomali Data Detection
import pandas as pd

#----------Data Preparation
df = pd.read_csv("E:/2.KULIAH/KP2_BBWS/DEBIT/tma_wonogiri.csv",header=None)

#colNaming
df.columns = ["DATE", "TIME", "OLD_VAL"]
#Combine column
df["DATETIME"] = df["DATE"].map(str) + " " + df["TIME"]
#Drop unused column
df.drop(["DATE","TIME"], axis=1, inplace=True)
#Reorder the column
df = df.reindex(columns=["DATETIME", "OLD_VAL"])

#sort data based on column DATETIME
df['DATETIME'] =  pd.to_datetime(df["DATETIME"], format='%Y-%m-%d %H:%M:%S')
df = df.sort_values(axis=0, ascending=True, by="DATETIME")
#reset index
df = df.reset_index(drop=True)

#Change DataFrame column into list
dflist = df["OLD_VAL"].values
#Use Standar Deviation to check the data later
deviasi = df["OLD_VAL"].std() 


#----------NEW ADD
#Change DataFrame column into list
dflist_tanggal = df["DATETIME"].tolist() #output = list of timestamp format
#dflist_tanggal = df["DATETIME"].values #output = list of numpy.datetime64 format
#----------


#Creating function to detect and replace outlier data
def anomali(list_value,list_tanggal,std) :
	jumlahdata = len(list_value)
	list_baru=[]
	for y in xrange(0,jumlahdata):
		anomali = True
		if y == 0 :
			#If first data on list
			while anomali == True :
				if (list_value[y] < list_value[y+1]-std) or (list_value[y] > list_value[y+1]+std) :
					if (list_value[y+1] < list_value[y+2]) : #-----------------------Jika perbandingan data setelahnya cenderung naik
						newdata = list_value[y+1]-((list_value[y+1]+list_value[y+2])/2)
						list_data = [list_tanggal[y],list_value[y],newdata]
						list_baru.append(list_data)
					elif (list_value[y+1] > list_value[y+2]) : #---------------------Jika perbandingan data setelahnya cenderung turun
						newdata = list_value[y+1]+((list_value[y+1]+list_value[y+2])/2)
						list_data = [list_tanggal[y],list_value[y],newdata]
						list_baru.append(list_data)
					else : #-------------------------------------------------Jika perbandingan data setelahnya sama
						newdata = list_value[y+1]
						list_data = [list_tanggal[y],list_value[y],newdata]
						list_baru.append(list_data)
				anomali = False
		elif y == (jumlahdata-1) :
			#If last data on list
			while anomali == True :
				if (list_value[y] < list_value[y-1]-std) or (list_value[y] > list_value[y-1]+std) :
					if (list_value[y-2] < list_value[y-1]) : #-----------------------Jika perbandingan data setelahnya cenderung naik
						newdata = list_value[y-1]+((list_value[y-1]+list_value[y-2])/2)
						list_data = [list_tanggal[y],list_value[y],newdata]
						list_baru.append(list_data)
					elif (list_value[y-2] > list_value[y-1]) : #---------------------Jika perbandingan data setelahnya cenderung turun
						newdata = list_value[y-1]-((list_value[y-1]+list_value[y-2])/2)
						list_data = [list_tanggal[y],list_value[y],newdata]
						list_baru.append(list_data)
					else : #-------------------------------------------------Jika perbandingan data setelahnya sama
						newdata = list_value[y+1]
						list_data = [list_tanggal[y],list_value[y],newdata]
						list_baru.append(list_data)
				anomali = False
		else :
			#If data is not first or last data
			while anomali == True :
				if ((list_value[y] < list_value[y-1]-std) and (list_value[y] < list_value[y+1]-std)) or ((list_value[y] > list_value[y-1]+std) and (list_value[y] > list_value[y+1]+std)) :
					newdata = (list_value[y-1]+list_value[y+1])/2
					list_data = [list_tanggal[y].strftime('%Y-%m-%d %H:%M:%S'),list_value[y],newdata] #-----strftime to convert timestamp into readable string
					list_baru.append(list_data)
				anomali = False
	return list_baru #----------Get list as return

#Declare list from function
data_anomali=anomali(dflist,dflist_tanggal,deviasi)

#Convert List into DataFrame
df_anomaly = pd.DataFrame(data_anomali, columns=["WAKTU", "NILAI_ASLI", "NILAI_USULAN"])

#Show the data
print(data_anomali)
print(len(data_anomali))
print(df_anomaly)

'''
#Write data into csv file
df_anomaly.to_csv("E:/2.KULIAH/KP2_BBWS/DEBIT/tma_usulan.csv",header=True,index=False)
'''