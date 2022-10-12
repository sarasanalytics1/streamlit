#from _typeshed import NoneType
from re import I
from numpy import NAN, NaN
import streamlit as st
import json
import pandas as pd
import os

#from streamlit.proto.Json_pb2 import Json
st.title("Json Parser")
st.subheader("Upload a json file to get the data in tabular format")
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
    s=list(data.keys())[0]
    #st.write(s)
    json_Data=pd.DataFrame([flatten_json(x) for x in data[s]])
    #st.dataframe(json_Data)

    for i in json_Data.columns:
    #print(str(type(json_Data[i].iloc[1])),i)

        if i == "payload_messages":
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



    #st.dataframe(json_Data)           
    msg_cols = [col for col in json_Data.columns if 'payload' in col]
    msg_col = msg_cols.append("_internal_adb_props.label")
    #msg_cols= list(msg_cols)
    df=json_Data[msg_cols]
    json_Data=df[df['_internal_adb_props.label'] == 'hitReceived']
    json_Data.reset_index(inplace=True,drop=True)
    #df=df.T
    #st.dataframe(json_Data)





    for i in json_Data.columns:
        if i=='payload_messages..event.xdm.productListItems':
            #st.write("productlist")
            s = json_Data.apply(lambda x: pd.Series(x[i]),axis=1).stack().reset_index(level=1, drop=True)
            s.name = i + '.'
            index_no = json_Data.columns.get_loc(i)
            json_Data=json_Data.drop([i],axis=1)
            json_Data=json_Data.join(s)
            count =count+1
            first_column = json_Data.pop(s.name)
            json_Data.insert(index_no, s.name, first_column)
            #st.write('list')
            #st.dataframe(json_Data)
            
    for i in json_Data.columns:
        if i == 'payload_messages..event.xdm.productListItems.':
        #if type(json_Data[i].iloc[3]) is dict:
            f=(json_Data[i].apply(pd.Series))
            #st.write('dict')
            #st.write(i)
            #st.dataframe(f)
            json_Data=json_Data.drop(i,axis=1)
            json_Data = pd.concat([json_Data, f], axis=1)
            #st.dataframe(json_Data)
            #st.write('done')
            #break
    
    json_Data.reset_index(inplace=True,drop=True)
    json_Data=json_Data.astype(str)
    #json_Data = json_Data.T
    #st.dataframe(json_Data)
    #st.write((json_Data['_merchVars'].iloc[1]))
    #json_Data['_merchVars']= json_Data['_merchVars'].astype('str')
    #for i in json_Data.columns:
        #if i == '_merchVars':
    json_Data['_merchVars'] = json_Data['_merchVars'].apply(lambda x: x.replace("\'", "\""))
    json_Data['_merchVars'] = json_Data['_merchVars'].apply(lambda x: json.loads(x) if x != "nan" else None)
    


    for i in json_Data.columns:
        if type(json_Data[i].iloc[3]) is dict:
            #st.write(i)
            f=(json_Data[i].apply(pd.Series))
            #st.write('dict')
            #st.dataframe(f)
            json_Data=json_Data.drop(i,axis=1)
            json_Data = pd.concat([json_Data, f], axis=1)
            #st.dataframe(json_Data)
            #st.write('done')
           
    json_Data['index']=json_Data.index
    #st.dataframe(json_Data)
    json_Data['index']=json_Data['index'].apply(lambda x: str(x))
    #st.write(type(json_Data['index'].iloc[3]))
    json_Data.index= json_Data['index']
    json_Data=json_Data.reindex(columns=['payload_messages..event.xdm.mobile.mobilePageDetails.siteRegion','payload_messages..event.xdm.mobile.mobilePageDetails.language',
    'payload_messages..xdm.mobile.mobilePageDetails.pageType','payload_messages..event.xdm.mobile.mobilePageDetails.siteSection','payload_messages..event.xdm.web.webPageDetails.name',
    'web.webPageDetails.pageViews','payload_messages..event.xdm.mobile.mobilePageDetails.pageViews.value','payload_messages..event.xdm.web.webInteraction.linkClicks.value','payload_messages..xdm.web.webInteraction.name',
    'payload_messages..event.xdm.web.webInteraction.type','topThingsChoice','otherOptions','includedSides','drinks','riceChoice','beansChoice','productName','proteinChoice','SKU','name','quantity','priceTotal','payload_messages..event.xdm.commerce.productListAdds.value',
    'payload_messages..event.xdm.commerce.productListOpens.value','payload_messages..event.xdm.mobile.mobilePageDetails.subPageName','payload_messages..event.xdm.shoppingCart.cartEdits.value','payload_messages..event.xdm.commerce.checkouts.value','payload_messages..event.xdm.commerce.checkoutFunnelInteractions.value','payload_ACPExtensionEventData_xdm_commerce_funnelName',
    'payload_messages..event.xdm.commerce.retrievalType','payload_messages..event.xdm.commerce.purchases.value','payload_messages..xdm.commerce.order.purchaseID','payload_messages..xdm.commerce.order.taxRevenue','payload_messages..xdm.commerce.order.feeRevenue','payload_messages..xdm.commerce.order.donationRevenue','payload_messages..event.xdm.commerce.order.groupOrderParticipants',
    'payload_messages..event.xdm.commerce.order.paymentMethod','payload_currency-code','payload_ACPExtensionEventData_xdm_commerce_order_pickupTime','payload_ACPExtensionEventData_xdm_commerce_checkoutType'])
    
    #st.write("before Transform")
    #st.dataframe(json_Data)
    json_Data = json_Data.T
    
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