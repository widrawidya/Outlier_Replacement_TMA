#Anomali Data Replacement
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

#----------Declaring needed data
print(df.describe())
#Change DataFrame column into list
dflist = df["OLD_VAL"].values
#Use Standar Deviation to check the data later
deviasi = df["OLD_VAL"].std() 

#Creating function to detect and replace outlier data
def anomali(list_,std) :
	jumlahdata = len(list_)
	list_baru=[]
	for y in xrange(0,jumlahdata):
		anomali = True
		if y == 0 :
			#If first data on list
			while anomali == True :
				if (list_[y] < list_[y+1]-std) or (list_[y] > list_[y+1]+std) :
					if (list_[y+1] < list_[y+2]) : #-----------------------Jika perbandingan data setelahnya cenderung naik
						newdata = list_[y+1]-((list_[y+1]+list_[y+2])/2)
						list_baru.append(newdata)
					elif (list_[y+1] > list_[y+2]) : #---------------------Jika perbandingan data setelahnya cenderung turun
						newdata = list_[y+1]+((list_[y+1]+list_[y+2])/2)
						list_baru.append(newdata)
					else : #-------------------------------------------------Jika perbandingan data setelahnya sama
						newdata = list_[y+1]
						list_baru.append(newdata)
				else :
					list_baru.append(list_[y])
				anomali = False
		elif y == (jumlahdata-1) :
			#If last data on list
			while anomali == True :
				if (list_[y] < list_[y-1]-std) or (list_[y] > list_[y-1]+std) :
					if (list_[y-2] < list_[y-1]) : #-----------------------Jika perbandingan data setelahnya cenderung naik
						newdata = list_[y-1]+((list_[y-1]+list_[y-2])/2)
						list_baru.append(newdata)
					elif (list_[y-2] > list_[y-1]) : #---------------------Jika perbandingan data setelahnya cenderung turun
						newdata = list_[y-1]-((list_[y-1]+list_[y-2])/2)
						list_baru.append(newdata)
					else : #-------------------------------------------------Jika perbandingan data setelahnya sama
						newdata = list_[y+1]
						list_baru.append(newdata)
				else :
					list_baru.append(list_[y])
				anomali = False
		else :
			#If data is not first or last data
			while anomali == True :
				if ((list_[y] < list_[y-1]-std) and (list_[y] < list_[y+1]-std)) or ((list_[y] > list_[y-1]+std) and (list_[y] > list_[y+1]+std)) :
					newdata = (list_[y-1]+list_[y+1])/2
					list_baru.append(newdata)
				else :
					list_baru.append(list_[y])
				anomali = False
	return list_baru #----------Get list as return

#Declare list from function
list_br=anomali(dflist,deviasi)

#Add new column into DataFrame
df["NEW_VAL"] = list_br
print(df)

print(df.describe())

'''
#Write data into csv file
df.drop(["OLD_VAL"], axis=1, inplace=True)
df.to_csv("E:/2.KULIAH/KP2_BBWS/DEBIT/tma_baru.csv",header=False,index=False)
'''