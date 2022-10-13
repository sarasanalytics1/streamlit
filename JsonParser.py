#from _typeshed import NoneType
from re import I
from numpy import NAN, NaN
import numpy as np
import streamlit as st
import json
import pandas as pd
import os
import datetime

#from streamlit.proto.Json_pb2 import Json
st.title("Json Parser")
st.subheader("Upload a json file to get the data in tabular format")
data_file = st.file_uploader("Choose a file",type='json')
#st.subheader("Upload Web json file below to get tabular format")

#web_file= st.file_uploader("Choose a  file",type='json')
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
        #df = df.reset_index()
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
           
    for i in json_Data.columns:
        if type(json_Data[i].iloc[1]) is dict:
            f=(json_Data[i].apply(pd.Series))
            #st.dataframe(f)
            json_Data=json_Data.drop([i],axis=1)
            json_Data = pd.concat([json_Data, f], axis=1)
    #st.write('final')
    #st.dataframe(json_Data)

    #st.dataframe(json_Data)           
    #msg_cols = [col for col in json_Data.columns if 'payload_ACP' in col]
    #msg_col = msg_cols.append("_internal_adb_props.label")
    #msg_cols= list(msg_cols)
    #df=json_Data[msg_cols]
    #st.dataframe(df)
    

    if "Web" in data_file.name:
        #st.write('web')
        json_Data=json_Data[json_Data['_internal_adb_props.label'] == 'Alloy Request']
    else:
        json_Data=json_Data[json_Data['_internal_adb_props.label'] == 'AEP Request Event']
        #st.write("data",json_Data.shape)

    #json_Data.reset_index(inplace=True,drop=True)
    #df=df.T
    #st.dataframe(json_Data)
    #json_Data= json_Data[json_Data['payload_messages.']!='Request received by Experience Edge.']
    #json_Data.drop(df_new,axis = 0 ,inplace= True)
    #st.dataframe(json_Data)
    





    for i in json_Data.columns:
        if i=='payload_ACPExtensionEventData_xdm_productListItems':
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
    json_Data['index']=json_Data.index
    json_Data.drop_duplicates(subset='index',inplace=True)
    #st.dataframe(json_Data)
    #st.write(json_Data.shape,"list")
            
    for i in json_Data.columns:
        if i == 'payload_ACPExtensionEventData_xdm_productListItems.':
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
    #st.dataframe(json_Data)
    #json_Data.reset_index(inplace=True,drop=True)
    json_Data=json_Data.astype(str)
    
    json_Data['_merchVars'] = json_Data['_merchVars'].apply(lambda x: x.replace("\'", "\""))
    json_Data['_merchVars'] = json_Data['_merchVars'].apply(lambda x: json.loads(x) if x != "nan" else None)
    
    #st.write('before merchvar',json_Data.shape)
    #st.dataframe(json_Data)

    for i in json_Data.columns:
        if i == '_merchVars':
        #if type(json_Data[i].iloc[3]) is dict:
            #st.write(i)
            f=(json_Data[i].apply(pd.Series))
            #f['index']=json_Data.index
            #f.drop_duplicates(inplace=True,subset='index')
            #st.write('dict')
            #st.dataframe(f)
            json_Data=json_Data.drop(i,axis=1)
            json_Data = pd.concat([json_Data, f], axis=1)
            json_Data.drop_duplicates(inplace=True,subset='index')
            #st.dataframe(json_Data)
            #st.write('done')
    #st.write('after _merchvar',json_Data.shape)
    #st.dataframe(json_Data)      
    json_Data.reset_index(inplace=True,drop=True)
    json_Data.drop('index',axis=1,inplace=True)
    #st.dataframe(json_Data) 
    #st.write(type(json_Data['index'].iloc[3]))
    json_Data.sort_values('timestamp',inplace=True)
    json_Data['timestamp'] = json_Data['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(int(str(x[0:10]))).isoformat())
    json_Data['Date']=json_Data['timestamp'].apply(lambda x:str(x)[:10])
    json_Data['Time']=json_Data['timestamp'].apply(lambda x:str(x)[12:])
    json_Data=json_Data.replace('nan', '')
    json_Data.reset_index(inplace=True,drop=True)
    json_Data.index = np.arange(1, len(json_Data) + 1)
    json_Data['Column Name']=json_Data.index
    
    json_Data.set_index('Column Name',inplace=True)
    #st.dataframe(json_Data)
    json_Data=json_Data.reindex(columns=['Consolidate with previous or Next Event',
    'KILL(its redundant or not needed)',
    'Whats this? "Special effect or missing event name "',
    'Notes',
    'Date',
    'Time',
    'payload_ACPExtensionEventData_xdm_eventType',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_language',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteSection',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteRegion',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageViews_value',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageType',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_subPageName',
    'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_previousPage',
    'payload_ACPExtensionEventData_xdm_web_webPageDetails_name',
    'payload_ACPExtensionEventData_xdm_web_webInteraction_type',
    'payload_ACPExtensionEventData_xdm_web_webInteraction_linkClicks_value',
    'payload_ACPExtensionEventData_xdm_web_webInteraction_name',
    'payload_ACPExtensionEventData_xdm_user_devicePlatform',
    'payload_ACPExtensionEventData_xdm_crewTipCount_value',
    'payload_ACPExtensionEventData_xdm_crewTipCount_value',
    'payload_ACPExtensionEventData_xdm_commerce_order_groupOrderParticipants',
    'payload_ACPExtensionEventData_xdm_commerce_order_purchaseID',
    'topThingsChoice','otherOptions','includedSides','drinks','riceChoice','beansChoice','productName','proteinChoice','SKU','name','quantity','priceTotal',
    'payload_ACPExtensionEventData_xdm_commerce_order_pickupTime',
    'payload_ACPExtensionEventData_xdm_commerce_order_taxRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_orderID',
    'payload_ACPExtensionEventData_xdm_commerce_order_donationRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_paymentMethod',
    'payload_ACPExtensionEventData_xdm_commerce_order_driverTipRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_crewTipRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_feeRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_purchases_value',
    'payload_ACPExtensionEventData_xdm_commerce_order_tipRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_checkoutType',
    'payload_ACPExtensionEventData_xdm_commerce_checkouts_value',
    'payload_ACPExtensionEventData_xdm_commerce_selectLocation_value',
    'payload_ACPExtensionEventData_xdm_commerce_retrievalType',
    'payload_ACPExtensionEventData_xdm_commerce_funnelName',
    'payload_ACPExtensionEventData_xdm_commerce_checkoutFunnelInteractions_value',
    'payload_ACPExtensionEventData_xdm_commerce_productListViews_value',
    'payload_ACPExtensionEventData_xdm_commerce_productListAdds_value',
    'payload_ACPExtensionEventData_xdm_commerce_productListRemovals_value',
    'payload_ACPExtensionEventData_xdm_commerce_productListOpens_value',
    'payload_ACPExtensionEventData_xdm_isDelivery',
    'payload_ACPExtensionEventData_xdm_storeLocator_storeID',
    'payload_ACPExtensionEventData_xdm_storeLocator_pickupLocation_value',
    'payload_ACPExtensionEventData_xdm_storeLocator_searchType',
    'payload_ACPExtensionEventData_xdm_storeLocator_locationSelected_value',
    'payload_ACPExtensionEventData_xdm_storeLocator_searchInitiated_value',
    'payload_ACPExtensionEventData_xdm_shoppingCart_cartEdits_value',
    'payload_ACPExtensionEventData_build.environment',
    'payload_ACPExtensionEventData_xdm_application_name',
    '_internal_adb_props.label'])
    json_Data.rename(columns={
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteRegion':'/web/webPageDetails/siteRegion',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_language': '/web/webPageDetails/Language',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageType' : '/web/webPageDetails/pageType',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteSection':'/web/webPageDetails/siteSection',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_name': '/web/webPageDetails/name',
        #'web.webPageDetails.pageViews': 
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_previousPage':'mobile/mobilePageDetails/previousPage',
        'payload_ACPExtensionEventData_xdm_user_devicePlatform':'user/devicePlatform',
        'payload_ACPExtensionEventData_xdm_crewTipCount_value': "crewTipCount_value",
        #'payload_ACPExtensionEventData_xdm_crewTipCount_value',
        'payload_ACPExtensionEventData_xdm_commerce_order_orderID' :'commerce/order/orderID',
        'payload_ACPExtensionEventData_xdm_commerce_order_driverTipRevenue':'commerce/order/driverTipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_order_crewTipRevenue':'commerce/order/crewTipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_order_tipRevenue':'commerce/order/tipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_selectLocation_value':'commerce/selectLocation/value',
        'payload_ACPExtensionEventData_xdm_commerce_productListViews_value':'commerce/productListViews/value',
        'payload_ACPExtensionEventData_xdm_commerce_productListRemovals_value':'commerce/productListRemovals/value',
        'payload_ACPExtensionEventData_xdm_isDelivery':'isDelivery',
        'payload_ACPExtensionEventData_xdm_storeLocator_storeID':'storeLocator_storeID',
        'payload_ACPExtensionEventData_xdm_storeLocator_pickupLocation_value':'storeLocator_pickupLocation_value',
        'payload_ACPExtensionEventData_xdm_storeLocator_searchType':'storeLocator_searchType',
        'payload_ACPExtensionEventData_xdm_storeLocator_locationSelected_value':'storeLocator_locationSelected_value',
        'payload_ACPExtensionEventData_xdm_storeLocator_searchInitiated_value':'storeLocator_searchInitiated_value',
        'payload_ACPExtensionEventData_build.environment':'build.environment',
        'payload_ACPExtensionEventData_xdm_application_name':'application/name',


        'payload_ACPExtensionEventData_xdm_eventType':'Event_Type',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageViews_value':'/web/webPageDetails/pageViews/value' ,
         'payload_ACPExtensionEventData_xdm_web_webInteraction_linkClicks_value':'/web/webInteraction/linkClicks/value',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_name': '/web/webInteraction/name',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_type':'/web/webInteraction/type',
        'topThingsChoice': '/productListItems/0/_merchVars/topThingsChoice',
        'otherOptions': '/productListItems/0/_merchVars/otherOptions',
        'includedSides': '/productListItems/0/_merchVars/includedSides',
        'drinks':'/productListItems/0/_merchVars/drinks',
        'riceChoice':'/productListItems/0/_merchVars/riceChoice',
        'beansChoice':'/productListItems/0/_merchVars/beansChoice',
        'productName':'/productListItems/0/_merchVars/productName',
        'proteinChoice':'/productListItems/0/_merchVars/proteinChoice',
        'SKU':'/productListItems/0/_merchVars/SKU',
        'name':'/productListItems/0/_merchVars/name',
        'quantity':'/productListItems/0/_merchVars/quantity',
        'priceTotal':'/productListItems/0/_merchVars/priceTotal',
        'payload_ACPExtensionEventData_xdm_commerce_productListAdds_value':'/commerce/productListAdds/value',
        'payload_ACPExtensionEventData_xdm_commerce_productListOpens_value':'/commerce/productListOpens/value',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_subPageName':'/web/webPageDetails/subPageName',
        'payload_ACPExtensionEventData_xdm_shoppingCart_cartEdits_value':'/shoppingCart/cartEdits/value',
        'payload_ACPExtensionEventData_xdm_commerce_checkouts_value': '/commerce/checkouts/value',
        'payload_ACPExtensionEventData_xdm_commerce_checkoutFunnelInteractions_value':'checkoutFunnelInteractions',
        'payload_ACPExtensionEventData_xdm_commerce_funnelName': '/commerce/funnelName',
    'payload_ACPExtensionEventData_xdm_commerce_retrievalType':'/commerce/retrievalType',
    'payload_ACPExtensionEventData_xdm_commerce_purchases_value': '/commerce/purchases/value',
    'payload_ACPExtensionEventData_xdm_commerce_order_purchaseID': '/commerce/order/purchaseID',
    'payload_ACPExtensionEventData_xdm_commerce_order_taxRevenue':'/commerce/order/taxRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_feeRevenue':'/commerce/order/feeRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_donationRevenue':'/commerce/order/donationRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_groupOrderParticipants':'/commerce/order/groupOrderParticipants',
    'payload_ACPExtensionEventData_xdm_commerce_order_paymentMethod':'/commerce/order/paymentMethod',
    #'payload_currency-code':'/commerce/order/currencyCode',
    'payload_ACPExtensionEventData_xdm_commerce_order_pickupTime':'/commerce/order/pickupTime',
    'payload_ACPExtensionEventData_xdm_commerce_checkoutType': '/commerce/checkoutType'

    },inplace=True)
    
    #json_Data['payload_ACPExtensionEventTimestamp'] = datetime.datetime.fromtimestamp(json_Data['payload_ACPExtensionEventTimestamp']).isoformat()
    
    
    #json_Data.drop_duplicates(inplace=True)
    #st.dataframe(json_Data)
    #st.write("before Transform")
    #st.dataframe(json_Data)
    

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