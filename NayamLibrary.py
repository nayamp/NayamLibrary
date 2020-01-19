import pandas as pd
import numpy as np
import csv
low_memory=False


## The StartingFilename is the filepath to the csv you want to run analysis on, the OutputFilename is whatever you want to name the output analysis file.
# StartingFilename = r'C:\Users\nayam_perez\hello\FADatabase-data.csv'
# OutputFilename = 'FA_Database_Analysis.csv'

# filepath='C:/Users/nayam_perez/hello/CPG_Cabinet/'
# filename='CPG_LineName_to_FLOC.csv'
# newfilename='Deduplicate_CPG_LineName_to_FLOC.csv'
# df=pd.read_csv(filepath+filename,'r',delimiter=',',index_col=False)

def analyzethis(StartingFilename,OutputFilename):
    #Here, I'm using the Pandas library's "read_csv" functionality to pull values from the file. I'm also specifying that this won't be a small file with "low_memory=False"
    df=pd.read_csv(StartingFilename, encoding='ISO-8859-1')
    #This next command is just looking through each column and finding out if they have any null values at all. I can modify this command to look for any sort of value, or multiple values.
    missing_info=list(df.columns[df.isnull().any()])
    #with open is a way to call a csv, and if it doesn't exist, it is created.
    with open(OutputFilename,'w', newline ='') as MissingNumberFile:
        MissingWriter = csv.writer(MissingNumberFile, delimiter = ',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        ColumnInfoForUse=[]
        total_rows=df.shape[0]
        total="Total Rows:" + str(total_rows)
        header=["Column Name", "Number of Values Missing", "Percentage Values Missing", total]
        MissingWriter.writerow(header)
        #MissingWriter is just the function to write to a csv. Don't worry about the "ColumnInfoForUse", That's something i'll be using for use within the program.
        #This for loop just computes the count of missing value for each column, and then computes the corresponding percentage.
        #Then, that info is turned into a row to be placed into your output csv.
        for col in missing_info:
            num_missing=str(df[df[col].isnull()==True].shape[0])
            PercentMissing=str((df[df[col].isnull()==True].shape[0]/df.shape[0])*100)
            ColumnInfo=[col,num_missing,PercentMissing]
            MissingWriter.writerow(ColumnInfo)
            ColumnInfoForUse.append(ColumnInfo)

#analyzethis(StartingFilename,OutputFilename)


def join_by_column(File1,File2,File1Columns,File2Index):
    
    A = pd.read_csv(File1, encoding='utf-8')
    B = pd.read_csv(File2,encoding='utf-8',delimiter=',')
    ##sometimes encoding = 'ISO-8859-1', sometimes skipline=1 on B

    A=A[[File1Columns]]
    B.columns=File2Index
    ##the above lines are two different examples of dataframe column modification. For A, we choose columns to keep from the original dataframe. For B, we rename the columns.

    merged = A.merge(B,how='inner', on='LINE',left_index=False,right_index=False,sort=False,copy=False,indicator=False)
    print(merged.head(),A.shape[0],'/n',B.shape[0],'/n',merged.shape[0])


    merged.to_csv(path_or_buf=r'C:\Users\nayam_perez\hello\CPGSTANDARDS_LINE_FLOC_deftest.csv',sep=',')

# File1='CPG_Cabinet/CPGSTANDARDS.csv'
# File2='CPG_Cabinet/Deduplicate_CPG_LineName_to_FLOC.csv'
# bcolumns=['LINE','RouteNumber','FLOCNumber']#rename the columns in b
# acolumns=['DWDOCID','LINE','WORKORDER','DOCUMENTTYPES','BOXID']#only use columns in a
# join_by_column(File1,File2,acolumns,bcolumns)


def Deduplicate_by_name(ColsToDrop,ColsToMerge,ColsToGroupby,Dataframe,Outfile):
    #Drop columns that aren't needed. If not needed, delete the ".drop()" portion, but leave the .astype(str). This function casts everything as a string, which is needed in the next step
    df=Dataframe.drop(ColsToDrop,axis=1).astype(str)

    #Enter the column name/s you'd like to erase duplicates of right after gropuby. The column/s you want rows merged should be right before the .apply statement.
    df=df.groupby(ColsToGroupby)[ColsToMerge].apply(','.join).reset_index()
    print(df.head())
    df.to_csv(Outfile,index=False)

# ColsToDrop='description'
# ColsToMerge='FLOCNumber'
# ColsToGroupby=['LINENAME','RouteNumber']
# Deduplicate_by_name(ColsToDrop,ColsToMerge,ColsToGroupby,df)


def unique_values_by_columns(Dataframe,column):
    #align dataframe name input df<>working df
    df=Dataframe
    #call all unique values, convert to list. 
    unique=df[column].unique().tolist()
    #this function is not callable, the syntax would be "YourListName" = unique_values_by_columns(Dataframe,column)
    return unique


def unique_values_df(Dataframe):
    #align dataframe name input df<>working df
    df=Dataframe
    #create an empty list of df names, that will populate while this function runs
    dfs=[]
    i=0
    #for each column, find all unique values.This computes all unique values of every column, 1 column each iteration.
    for col in df.columns:
        dfs.append(('df_'+'%s',col))
        dfs[i]=df[col].unique().tolist()
        dfs[i]=pd.Series(dfs[i])
        i+=1 
    #create a df consisting of all unique values in each column.
    df_out=pd.concat(dfs,ignore_index=True,axis=1)
    #keep column names from original df
    df_out.columns=df.columns 
    #this function is not callable, the syntax would be "YourDataframeName" = unique_values_df(Dataframe)
    return df_out


def create_dictionary_from_csv(mapping_file):
    #open csv containing 2 columns, one with values to be replaced, and the other with wanted values
    #encoding might need to be changed to 'ISO-8859-1' if error occurs
    with open(mapping_file, 'r', encoding='UTF-8-sig',newline='') as mapfile:   
        reader=csv.reader(mapfile)
        mapdict={rows[0]:rows[1] for rows in reader}
    #this function is not callable, it returns a dictionary object. syntax is 'MyDict=create_dictionary_from_csv('filename.csv')
    return mapdict
        
def remap_column_values(Dataframe, ColumnToReplace, dictionary):
    #align input df<>working df
    df=Dataframe
    #align input dictionary<>working dictionary
    mapdict=dictionary
    #map values from dictionary to df column, replace columns that are in dictionary, keep original values if not found in dictionary. 
    # #If you want to drop values that are not found in dictionary, replace df[ColumnToReplace] inside of fillna() with whatever value you'd like to replace with in quotes
    df[ColumnToReplace]=df[ColumnToReplace].map(mapdict).fillna(df[ColumnToReplace])
    #this function is not callable, the syntax would be "YourDataframeName" = remap_column_values(Dataframe, 'ColumnToReplace', DictionaryName)
    return df

def filter_from_csv(filein,fileout,object_to_replace):
    FilteredFile=[]
    with open(filein, 'r', encoding='ISO-8859-1') as myfile:
        filtered = (line.replace(object_to_replace, ' ') for line in myfile)
        for row in csv.reader(filtered):
            FilteredFile.append(row)
    with open(fileout, 'w', encoding = 'UTF-8',newline ='') as myfile:
        wr=csv.writer(myfile,quoting=csv.QUOTE_ALL)
        for row in FilteredFile:
            wr.writerow(row)


def column_relationships(Dataframe,column1,column2):
    df=Dataframe[[column1,column2]]
    column1_unique_values=df[column1].unique().tolist()
    allfor=[]
    for value in column1_unique_values:
        subvaluedf=df.loc[df[column1]==value]
        column2_unique_subvalues=subvaluedf[column2].unique().tolist()
        n=len(column2_unique_subvalues)
        unique_supervalues=list(zip([value]*n,list(column2_unique_subvalues)))
        
        allfor.extend(unique_supervalues)

    count=np.array(df[column2].value_counts(dropna=False))
    #count=list(df[column2].groupby(column2).count())
    
    arrays=pd.MultiIndex.from_tuples(allfor,names=[column1,column2])

    s=pd.Series(count,index=arrays)
    return s

# File1='fruits_test.csv'
# framename=pd.read_csv(File1, encoding='utf-8', delimiter=',',index_col=False)
# need1='Fruit'
# need2='Subspecies'
# multii=column_relationships(framename,need1,need2)
# print(multii)
