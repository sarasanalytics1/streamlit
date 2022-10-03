from re import I
import streamlit as st
import json
import pandas as pd
import os
st.title("Hi, welcome to Saras")
data_file = st.file_uploader("Choose a file",type='json')
count = 0
import ast

def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if "Web" not in data_file.name:
            if type(x) is dict:
                for a in x:
                    if a not in exclude: flatten(x[a], name + a + '_')
            else:
                out[name[:-1]] = x
        else:
            if type(x) is dict:
                for a in x:
                    if a not in exclude: flatten(x[a], name + a + '_')
            elif type(x) is list:
                x=str(x)

            else:
                out[name[:-1]] = x

    flatten(nested_json)
    return out

def flatten_nested_json_df(df):
        df = df.reset_index()
        s = (df.applymap(type) == list).all()
        list_columns = s[s].index.tolist()
    
        s = (df.applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

    
        while len(list_columns) > 0 or len(dict_columns) > 0:
            new_columns = []

            for col in dict_columns:
                horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}.')
                horiz_exploded.index = df.index
                df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
                new_columns.extend(horiz_exploded.columns) # inplace

            for col in list_columns:
                print(f"exploding: {col}")
                df = df.drop(columns=[col]).join(df[col].explode().to_frame())
                new_columns.append(col)

            s = (df[new_columns].applymap(type) == list).all()
            list_columns = s[s].index.tolist()

            s = (df[new_columns].applymap(type) == dict).all()
            dict_columns = s[s].index.tolist()
        return df
if data_file == None:
    st.write("Please upload the File")
else:
    data = json.load(data_file)
    #st.write(data)
    json_Data=pd.DataFrame([flatten_json(x) for x in data['events']])
    #st.write(json_Data)
    for i in json_Data.columns:
    #print(str(type(json_Data[i].iloc[1])),i)
        if type(json_Data[i].iloc[1]) is list:
            #st.write(i)
            s = json_Data.apply(lambda x: pd.Series(x[i]),axis=1).stack().reset_index(level=1, drop=True)
            s.name = i + '.'
            #st.write(i)
            #st.write(s.name)
            index_no = json_Data.columns.get_loc(i)
            json_Data=json_Data.drop(i,axis=1)
            #st.dataframe(s)
            json_Data=json_Data.join(s)
            count =count+1
            #st.write(s)
            first_column = json_Data.pop(s.name)
            json_Data.insert(index_no, s.name, first_column)
            #st.dataframe(json_Data)


        if count >= 1:
            break

    #st.dataframe(hi["payload_messages."])
    #st.write((type(hi['payload_messages.'].iloc[8])))
    for i in json_Data.columns:
        if type(json_Data[i].iloc[1]) is dict:
            f=(json_Data[i].apply(pd.Series))
            #st.dataframe(f)
            json_Data=json_Data.drop([i],axis=1)
            json_Data = pd.concat([json_Data, f], axis=1)
    #st.write('final')
    #st.dataframe(json_Data)



    for i in json_Data.columns:
        if i == "payload_messages.":
            #st.write("Hi")
            json_Data['index'] = json_Data.index
            hi=(json_Data[json_Data["payload_messages."].str.contains('"event"').fillna(False)])
            #st.dataframe(hi)
            json_Data["payload_messages."] = json_Data["payload_messages."].astype('str')
            #json_Data["payload_messages."] = json_Data["payload_messages."].apply(lambda x: json.loads(x) if json_Data["payload_messages."].str.contains('"event"').fillna(False) else json_Data["payload_messages."] )
            hi["payload_messages."] = hi["payload_messages."].astype('str')
            hi["payload_messages."] = hi["payload_messages."].apply(lambda x: json.loads(x))
            #st.write("hi",hi.shape[0])
            df2 = flatten_nested_json_df(hi)
            df2.drop_duplicates(inplace=True,subset='uuid')
            #f2 = pd.json_normalize(hi["payload_messages."])
            #st.write('df2')
            #st.dataframe(df2)
            #st.write("df2",df2.shape)
            non_event = json_Data[~json_Data["payload_messages."].str.contains('"event"').fillna(False)]
            #st.write("Non_Event")
            #st.dataframe(non_event)
            #st.write("non_event",non_event.shape)
            main = df2.append(non_event, ignore_index = True)
            #st.write("main",main.shape)
            #st.write('Main')
            main = main.dropna(axis = 1, how = 'all')
            #main = main.drop(['index'],axis=1)
            #st.dataframe(main)
            #st.write("main",main.shape)
            #main = main.sort_values('index')
            main = main.set_index('index')
            json_Data=main.sort_index(ascending=True)
            for i in json_Data.columns:
                if i == "level_0":
                    json_Data = json_Data.drop("level_0",axis=1)
    st.dataframe(json_Data)           

    st.write("Filename: ", data_file.name)
    st.write("output file name",str(data_file.name)[:-5]+"_output.csv")
    #st.write(os.sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'HasOffers_POSTCalls')))
    output = str(data_file.name)[:-5]+"_output.csv"
    #csv= .to_csv(output)


    def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')


    csv = convert_df(json_Data)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name= output,
        mime='text/csv',
    )
