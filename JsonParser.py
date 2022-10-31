#from _typeshed import NoneType
from re import I
from numpy import NAN, NaN
import numpy as np
from pyparsing import java_style_comment
import streamlit as st
import json
import pandas as pd
import os
import xlsxwriter
import pytz
from io import BytesIO

from datetime import datetime
t='<h2 style= color:Red; font-size: 10px;">For IOS And Android upload Json below</h2>'
#from streamlit.proto.Json_pb2 import Json
st.title("Json Parser")
st.markdown(t,unsafe_allow_html=True)
n='<h2 style= color:Blue; font-size: 10px;">For Web upload  Json below</h2>'
data_file = st.file_uploader("Choose a file",type='json')
n=st.markdown(n,unsafe_allow_html=True)
web_file= st.file_uploader("Choose a  file",type='json')
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

def style_specific_cell(x):
    df = x.copy()
    color = 'background-color:#FFF974'
    df1 = pd.DataFrame('', index=x.index, columns=x.columns)
    df1.iloc[0,2] = color
    df1.iloc[0,4]=color
    df1.iloc[0,6]=color
    df1.iloc[0,8]=color
    for i in range(1,df.shape[0]-1):
        for j in range(1,df.shape[1]-1):
            if df.loc[i,j]=="Kill (duplicate)" :
                df1.loc[i,j]='background-color: #FFCCCB'
            elif df.loc[i,j]=="Kill (Don't track)":
                df1.loc[i,j]='background-color: #FFCCCB'
            elif df.loc[i,j]=='Consolidate':
                df1.loc[i,j]='background-color: lightorange'
    
    return df1    

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
    #st.dataframe(json_Data)
    json_Data['timestamp'] = pd.to_datetime(json_Data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
    #st.dataframe(json_Data)
    #json_Data['timestamp']= pd.to_datetime(json_Data['timestamp'].tz_convert(pytz.timezone('US/Pacific')))
    
    #st.write((json_Data['timestamp'].iloc[1]))
    #json_Data['timestamp'] = json_Data['payload_ACPExtensionEventTimestamp'].apply(lambda x: datetime.datetime.fromtimestamp(int(str(x[0:12]))).isoformat())
    json_Data['Date']=json_Data['timestamp'].apply(lambda x:str(x)[:10])
    json_Data['Time/PST']=json_Data['timestamp'].apply(lambda x:str(x)[11:19])
    json_Data['Time/PST']=json_Data['Time/PST'].apply(lambda x: datetime.strptime(x, "%H:%M:%S").strftime("%I:%M :%S %p"))
    #st.dataframe(json_Data)
    #st.write(type(json_Data['timestamp'].iloc[1]))
    json_Data=json_Data.replace('nan', '')
    json_Data.reset_index(inplace=True,drop=True)
    json_Data.index = np.arange(1, len(json_Data) + 1)
    
    
    
    #st.dataframe(json_Data)
    json_Data=json_Data.reindex(columns=['Web','Reviewed with Devs?','User Story #',
    'Kill or Consolidate?',
    'Recommendation',
    'Notes','Server Hits',
    'Date',
    'Time/PST',
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
    #'payload_ACPExtensionEventData_xdm_crewTipCount_value',
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
        'Time/PST':'Time(PST Timezone)',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteRegion':'web.webPageDetails.siteRegion',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_language': 'web.webPageDetails.Language',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageType' : 'web.webPageDetails.pageType',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteSection':'web.webPageDetails.siteSection',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_name': 'web.webPageDetails.name',
        #'web.webPageDetails.pageViews': 
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_previousPage':'mobile.mobilePageDetails.previousPage',
        'payload_ACPExtensionEventData_xdm_user_devicePlatform':'user.devicePlatform',
        'payload_ACPExtensionEventData_xdm_crewTipCount_value': "crewTipCount_value",
        #'payload_ACPExtensionEventData_xdm_crewTipCount_value',
        'payload_ACPExtensionEventData_xdm_commerce_order_orderID' :'commerce.order.orderID',
        'payload_ACPExtensionEventData_xdm_commerce_order_driverTipRevenue':'commerce.order.driverTipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_order_crewTipRevenue':'commerce.order.crewTipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_order_tipRevenue':'commerce.order.tipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_selectLocation_value':'commerce.selectLocation.value',
        'payload_ACPExtensionEventData_xdm_commerce_productListViews_value':'commerce.productListViews.value',
        'payload_ACPExtensionEventData_xdm_commerce_productListRemovals_value':'commerce.productListRemovals.value',
        'payload_ACPExtensionEventData_xdm_isDelivery':'isDelivery',
        'payload_ACPExtensionEventData_xdm_storeLocator_storeID':'storeLocator.storeID',
        'payload_ACPExtensionEventData_xdm_storeLocator_pickupLocation_value':'storeLocator.pickupLocation.value',
        'payload_ACPExtensionEventData_xdm_storeLocator_searchType':'storeLocator.searchType',
        'payload_ACPExtensionEventData_xdm_storeLocator_locationSelected_value':'storeLocator.locationSelected.value',
        'payload_ACPExtensionEventData_xdm_storeLocator_searchInitiated_value':'storeLocator.searchInitiated.value',
        'payload_ACPExtensionEventData_build.environment':'build.environment',
        'payload_ACPExtensionEventData_xdm_application_name':'application.name',


        'payload_ACPExtensionEventData_xdm_eventType':'Event.Type',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageViews_value':'web.webPageDetails.pageViews.value' ,
         'payload_ACPExtensionEventData_xdm_web_webInteraction_linkClicks_value':'web.webInteraction.linkClicks.value',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_name': 'web.webInteraction.name',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_type':'web.webInteraction.type',
        'topThingsChoice': 'productListItems0.merchVars.topThingsChoice',
        'otherOptions': 'productListItems0.merchVars.otherOptions',
        'includedSides': 'productListItems0.merchVars.includedSides',
        'drinks':'productListItems0.merchVars.drinks',
        'riceChoice':'productListItems0.merchVars.riceChoice',
        'beansChoice':'productListItems0.merchVars.beansChoice',
        'productName':'productListItems0.merchVars.productName',
        'proteinChoice':'productListItems0.merchVars.proteinChoice',
        'SKU':'productListItems0.merchVars.SKU',
        'name':'productListItems0.merchVars.name',
        'quantity':'productListItems0.merchVars.quantity',
        'priceTotal':'productListItems0.merchVars.priceTotal',
        'payload_ACPExtensionEventData_xdm_commerce_productListAdds_value':'commerce.productListAdds.value',
        'payload_ACPExtensionEventData_xdm_commerce_productListOpens_value':'commerce.productListOpens.value',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_subPageName':'web.webPageDetails.subPageName',
        'payload_ACPExtensionEventData_xdm_shoppingCart_cartEdits_value':'shoppingCart.cartEdits.value',
        'payload_ACPExtensionEventData_xdm_commerce_checkouts_value': 'commerce.checkouts.value',
        'payload_ACPExtensionEventData_xdm_commerce_checkoutFunnelInteractions_value':'checkoutFunnelInteractions',
        'payload_ACPExtensionEventData_xdm_commerce_funnelName': 'commerce.funnelName',
    'payload_ACPExtensionEventData_xdm_commerce_retrievalType':'commerce.retrievalType',
    'payload_ACPExtensionEventData_xdm_commerce_purchases_value': 'commerce.purchases.value',
    'payload_ACPExtensionEventData_xdm_commerce_order_purchaseID': 'commerce.order.purchaseID',
    'payload_ACPExtensionEventData_xdm_commerce_order_taxRevenue':'commerce.order.taxRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_feeRevenue':'commerce.order.feeRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_donationRevenue':'commerce.order.donationRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_groupOrderParticipants':'commerce.order.groupOrderParticipants',
    'payload_ACPExtensionEventData_xdm_commerce_order_paymentMethod':'commerce.order.paymentMethod',
    #'payload_currency-code':'/commerce/order/currencyCode',
    'payload_ACPExtensionEventData_xdm_commerce_order_pickupTime':'commerce.order.pickupTime',
    'payload_ACPExtensionEventData_xdm_commerce_checkoutType': 'commerce.checkoutType'

    },inplace=True)
    
    #json_Data['payload_ACPExtensionEventTimestamp'] = datetime.datetime.fromtimestamp(json_Data['payload_ACPExtensionEventTimestamp']).isoformat()
    
    
    #json_Data.drop_duplicates(inplace=True)
    #st.dataframe(json_Data)
    #st.write("before Transform")
    #st.dataframe(json_Data)
    
    n=json_Data[json_Data.duplicated()]
    #st.write(n.index)
    for i in n.index:
        json_Data.loc[i,'Notes']='Duplicate'
    json_Data['Server Hits']=json_Data.index
    json_Data.loc[1,'Web']= '# of server calls found:'
    #json_Data.loc[2,'Web']= json_Data.index.shape
    json_Data.loc[3,'Web']= ' # we can Kill or Consolidate:'
    #json_Data.loc[4,'Web']= json_Data['Kill or Consolidate?'].sum()
    json_Data.loc[5,'Web']= '# that are Valid or for Discusson:'
    #json_Data.loc[6,'Web']= json_Data.index.shape-json_Data['Kill or Consolidate?'].sum()
    json_Data.loc[7,'Web']= '% possible reduction:'
    #json_Data.loc[8,'Web']= str(1-(json_Data.loc[6,'Web']/json_Data.index.shape))[1:7]+"%"
    #st.dataframe(n)
    #st.dataframe(json_Data)
    json_Data = json_Data.T
    json_Data=json_Data.reset_index()
    #json_Data.columns = json_Data.iloc[0]
    #json_Data = json_Data.reindex(json_Data.index.drop(0)).reset_index(drop=True)
    #json_Data.columns.name = None
    st.write("Filename: ", data_file.name)
    st.write("output file name",str(data_file.name)[:-5]+"_Output.csv")
    #st.write(os.sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'HasOffers_POSTCalls')))
    output = str(data_file.name)[:-5]+"_Output.xlsx"
    #csv= .to_csv(output)
    hit_count=json_Data.shape[1]
    output1 = BytesIO()
    with pd.ExcelWriter(output1, engine='xlsxwriter') as writer:
        format_r = writer.book.add_format({'bg_color':   '#FFF974',
                          'font_color': 'black',
                          'font_name':'Arial Black',
                          'bold':'True'})
        format_n = writer.book.add_format({'bg_color':   '#FFCCCB',
                          'font_color': 'black',
                          'bold':'True'})
        format_k= writer.book.add_format({
                          'font_color': 'black',
                          'font_name':'Arial Black',
                          'font_size':'11',
                          'bold':'True'})
        format_c= writer.book.add_format({
                           'bg_color':   '#FFCCCB',
                          'font_color': 'red',
                          'font_name':'Arial',
                          'font_size':'9'})
        format_g= writer.book.add_format({
                           'bg_color':   '#90EE90',
                          'font_color': 'green',
                          'font_name':'Arial',
                          'font_size':'9'})
        format_o= writer.book.add_format({
                           'bg_color':   '#FFD580',
                          'font_color': 'orange',
                          'font_name':'Arial',
                          'font_size':'9'})
        format_co= writer.book.add_format({
                           'bg_color':   '#FFF974',
                          'font_color': 'black',
                          'font_name':'Arial',
                          'font_size':'9'})
    # Write each dataframe to a different worksheet.
        json_Data.to_excel(writer,header=0,index=0,sheet_name='Sheet1')
        for column in json_Data:
            column_width = max(json_Data[column].astype(str).map(len).max(), len(str(column)))
            col_idx = json_Data.columns.get_loc(column)
            #st.write(col_idx)
            writer.sheets['Sheet1'].set_column(col_idx, col_idx, column_width)
        worksheet=writer.sheets['Sheet1']
        worksheet.write('C1',hit_count,format_r)
        from string import ascii_uppercase as alc
        n=alc
        
        for i in alc:
            worksheet.write_formula(i+str(4), '{IFERROR(FIND("kill",LOWER('+i+str(5)+')),0)+IFERROR(FIND("consolidate",LOWER('+i+str(5)+')),0)}')
            for j in n:
                worksheet.write_formula(i+j+str(4), '{IFERROR(FIND("kill",LOWER('+i+j+str(5)+')),0)+IFERROR(FIND("consolidate",LOWER('+i+j+str(5)+')),0)}')
        worksheet.write_formula('E1', '{=SUM(B4:ZZ4)}',format_r)
        worksheet.write_formula('G1','{=(C1-E1)}',format_r)
        worksheet.write_formula('I1','{=1-(G1/C1)}',format_r)
        worksheet.write('A1',str(data_file.name)[:-5],format_r)  
        worksheet.write('A4','Kill or Consolidate?') 
        from xlsxwriter.utility import xl_rowcol_to_cell
        worksheet.data_validation(4,1,4,hit_count, {'validate': 'list',
                                  'source': ['Consolidate', 'Kill (Duplicate)', 'Kill (Conditionally)',"Kill (Don't track)",'OK','OK w/value bug','Discuss']})
        worksheet.conditional_format('A1:A100', {'type': 'cell',
                                    'criteria': 'not equal to',
                                    'value':    1,
                                    'format':   format_k})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Kill (Duplicate)"',
                                    'format':   format_c})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Kill (Conditionally)"',
                                    'format':   format_c})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '''"Kill (Don't track)"''',
                                    'format':   format_c})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"OK"',
                                    'format':   format_g})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"OK w/value bug"',
                                    'format':   format_g})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Discuss"',
                                    'format':   format_o})
        worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Consolidate"',
                                    'format':   format_co})
        worksheet.freeze_panes(1, 1)



    writer.save()
    
    st.download_button(
        label="Download data as Excel",
        data=output1,
        file_name= output,
       mime="application/vnd.ms-excel",
    )






def flatten_json_web(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten_web(x, name='', exclude=exclude):
        if "Web" not in web_file.name:
            if type(x) is dict:
                for a in x:
                    if a not in exclude: flatten_web(x[a], name + a + '_')
            else:
                out[name[:-1]] = x
        else:
            if type(x) is dict:
                for a in x:
                    if a not in exclude: flatten_web(x[a], name + a + '_')

            else:
                out[name[:-1]] = x

    flatten_web(nested_json)
    return out



if web_file == None:
    pass
else:
    data_web = json.load(web_file)
    s=list(data_web.keys())[0]
    #st.write(s)
    json_Data=pd.DataFrame([flatten_json_web(x) for x in data_web[s]])
    t=json_Data
    
    
    #st.dataframe(json_Data)           
    #msg_cols = [col for col in json_Data.columns if 'payload_ACP' in col]
    #st.write(msg_cols)
    #msg_cols= (msg_cols)+["_internal_adb_props.label"]
    #msg_cols= msg_cols.append("_internal_adb_props.label")
    #st.write(msg_cols)
    #msg_cols= msg_cols.append()
    #msg_cols=msg_cols.append()
    #msg_cols= (msg_cols)+["timestamp"]

    #json_Data=json_Data[msg_cols]
    #st.write('acp')
    #st.dataframe(json_Data)
    

  
    json_Data=json_Data[json_Data['_internal_adb_props.label'] == 'Alloy Request']
    





    for i in json_Data.columns:
        if i=='payload_ACPExtensionEventData_xdm_productListItems':
            #st.write("productlist")
            s = json_Data.apply(lambda x: pd.Series(x[i]),axis=1).stack().reset_index(level=1, drop=True)
            s.name = i + '.'
            index_no = json_Data.columns.get_loc(i)
            json_Data=json_Data.drop([i],axis=1)
            json_Data=json_Data.join(s)
            first_column = json_Data.pop(s.name)
            json_Data.insert(index_no, s.name, first_column)
            #st.write('list')
            #st.dataframe(json_Data)
    json_Data['index']=json_Data.index
    json_Data.drop_duplicates(subset='index',inplace=True)
    #st.dataframe(json_Data)
    #st.write(json_Data.shape)
            
    for i in json_Data.columns:
        if i == 'payload_ACPExtensionEventData_xdm_productListItems.':
        #if type(json_Data[i].iloc[3]) is dict:
            f=(json_Data[i].apply(pd.Series))
            
            json_Data=json_Data.drop(i,axis=1)
            json_Data = pd.concat([json_Data, f], axis=1)
            #st.write(len(list(json_Data.columns)))
            #st.write(len(set(list(json_Data.columns))))
            from collections import Counter
            counts = dict(Counter(json_Data.columns))
            duplicates = {key:value for key, value in counts.items() if value > 1}
            #st.write('fgs')
            #st.write(duplicates)
    #st.dataframe(json_Data)
    #json_Data.reset_index(inplace=True,drop=True)
    #json_Data=json_Data.astype(str)
    
    #json_Data['_merchVars'] = json_Data['_merchVars'].apply(lambda x: x.replace("\'", "\""))
    #json_Data['_merchVars'] = json_Data['_merchVars'].apply(lambda x: json.loads(x) if x != "nan" else None)
    
    #st.write('before merchvar',json_Data.shape)
    #st.dataframe(json_Data)

    for i in json_Data.columns:
        if i == '_merchVars':
        #if type(json_Data[i].iloc[3]) is dict:
            #st.write(i)
            f=(json_Data[i].apply(pd.Series))
            #f['index']=json_Data.index
            
            
            #json_Data['index']=json_Data.index
            
            #st.write(json_Data.index.duplicated())
            json_Data = pd.concat([json_Data, f], axis=1)
            json_Data.dropna(how='all', axis=1,inplace=True)
            from collections import Counter
            counts = dict(Counter(json_Data.columns))
            duplicates = {key:value for key, value in counts.items() if value > 1}
            #st.write('index')
            #st.write(duplicates)
            #json_Data.rename(columns={'0':'waste'},inplace=True)
    #st.write('after _merchvar',json_Data.shape)
    #st.dataframe(json_Data)      
    json_Data.reset_index(inplace=True,drop=True)
    json_Data.drop('index',axis=1,inplace=True)
    #st.dataframe(json_Data) 
    #st.write(type(json_Data['index'].iloc[3]))
    json_Data.sort_values('timestamp',inplace=True)
    json_Data['timestamp'] = pd.to_datetime(json_Data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
    #st.dataframe(json_Data)
    #json_Data['timestamp'] = json_Data['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(int(str(x[0:10]))).isoformat())
    json_Data['Date']=json_Data['timestamp'].apply(lambda x:str(x)[:10])
    json_Data['Time/PST']=json_Data['timestamp'].apply(lambda x:str(x)[11:19])
    json_Data['Time/PST']=json_Data['Time/PST'].apply(lambda x: datetime.strptime(x, "%H:%M:%S").strftime("%I:%M :%S %p"))
    #st.dataframe(json_Data)
    json_Data=json_Data.replace('nan', '')
    json_Data.reset_index(inplace=True,drop=True)
    json_Data.index = np.arange(1, len(json_Data) + 1)
    
    #st.write(len(json_Data.columns))
    #st.write(len(set(json_Data.columns)))
    
    #st.dataframe(json_Data)
    json_Data=json_Data.reindex(columns=['Web','Reviewed with Devs?','User Story #',
    'Kill or Consolidate?',
    'Recommendation',
    'Notes',
    'Server Hits',
    'Date',
    'Time/PST',
    'payload_ACPExtensionEventData_xdm_eventType',
    'payload_ACPExtensionEventData_xdm_web_webPageDetails_name',
    'payload_ACPExtensionEventData_xdm_web_pageViews_value',
'payload_ACPExtensionEventData_xdm_web_menuPageViews_value',
'payload_ACPExtensionEventData_xdm_web_webPageDetails_subSection1',
'payload_ACPExtensionEventData_xdm_web_webPageDetails_subSection2',
'payload_ACPExtensionEventData_xdm_web_webPageDetails_page',
'payload_ACPExtensionEventData_xdm_web_webPageDetails_subPageName',
'payload_ACPExtensionEventData_xdm_web_webPageDetails_subPageViews_value',
'payload_ACPExtensionEventData_xdm_web_webReferrer_URL',
'payload_ACPExtensionEventData_xdm_web_siteRegion',
'payload_ACPExtensionEventData_xdm_web_language',
'payload_ACPExtensionEventData_xdm_web_URL',
'payload_ACPExtensionEventData_xdm_web_pageType',
'payload_ACPExtensionEventData_xdm_web_siteSection',
'payload_ACPExtensionEventData_xdm_web_name',
'payload_ACPExtensionEventData_xdm_web_subSection1',
'payload_ACPExtensionEventData_xdm_web_subSection2',
'payload_ACPExtensionEventData_xdm_web_page',
'payload_ACPExtensionEventData_xdm_web_subPageName',
'payload_ACPExtensionEventData_xdm_web_subPageViews_value',
'payload_ACPExtensionEventData_xdm_web_webInteraction_linkModuleName',
'payload_ACPExtensionEventData_xdm_web_webInteraction_name',
'payload_ACPExtensionEventData_xdm_web_webInteraction_type',
'payload_ACPExtensionEventData_xdm_web_webInteraction_webInteraction_ctaClicks_value',
'payload_ACPExtensionEventData_xdm_web_webInteraction_linkClicks_value',
'payload_ACPExtensionEventData_xdm_web_webInteraction_type',
'payload_ACPExtensionEventData_xdm__chipotle_loginStatus',
'payload_ACPExtensionEventData_xdm__chipotle_devicePlatform',
'payload_ACPExtensionEventData_xdm__chipotle_orderNowClicks_value',
'payload_ACPExtensionEventData_xdm__chipotle_errors_value',
'payload_ACPExtensionEventData_xdm__chipotle_errorName',
'payload_ACPExtensionEventData_xdm__chipotle_storeId',
'payload_ACPExtensionEventData_xdm__chipotle_deliveryAddressSubmitted_value',
'payload_ACPExtensionEventData_xdm__chipotle_formNameDetails',
'payload_ACPExtensionEventData_xdm__chipotle_loginType',
'payload_ACPExtensionEventData_xdm__chipotle_accountInteraction_value',
'payload_ACPExtensionEventData_xdm__chipotle_registrationType',
'payload_ACPExtensionEventData_xdm__chipotle_interactionName',
'payload_ACPExtensionEventData_xdm__chipotle_addAddressDetails_value',
'payload_ACPExtensionEventData_xdm__chipotle_geoSearched',
'payload_ACPExtensionEventData_xdm__chipotle_deliveryAddressLookup_value',
'payload_ACPExtensionEventData_xdm__chipotle_searchType',
'payload_ACPExtensionEventData_xdm__chipotle_cartEdits_value',
'payload_ACPExtensionEventData_xdm__chipotle_mealNameGiven_value',
'payload_ACPExtensionEventData_xdm__chipotle_orderDuplications_value',
'payload_ACPExtensionEventData_xdm__chipotle_orderDuplicates_value',
'payload_ACPExtensionEventData_xdm__chipotle_availablePoints',
'payload_ACPExtensionEventData_xdm__experience_analytics_customDimensions_eVars_eVar60',
'payload_ACPExtensionEventData_xdm__experience_analytics_customDimensions_eVars_eVar47',
'payload_ACPExtensionEventData_xdm__experience_analytics_customDimensions_eVars_eVar9',
'payload_ACPExtensionEventData_xdm_marketing_internalCampaign',
'payload_ACPExtensionEventData_xdm_marketing_internalCampaignClicks_value',
'payload_ACPExtensionEventData_xdm_web_webInteraction_webInteraction_ctaNameType',
'topThingsChoice','otherOptions','includedSides','drinks','riceChoice','beansChoice','productName','proteinChoice','SKU','name','quantity','priceTotal',
    '_internal_adb_props.label'
    ])
    json_Data.rename(columns={
        'Time/PST':'Time(PST Timezone)',
        'payload_ACPExtensionEventData_xdm_web_pageViews_value':'web.pageViews.value',
        'payload_ACPExtensionEventData_xdm_web_menuPageViews_value':'web.MenuPageViews.Value',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_subSection1':'web.PageDetails.subSection1',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_subSection2':'web.PageDetails.subSection2',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_page':'web.webPageDetails.page',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_subPageName': 'web.webPageDetails.subPageName',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_subPageViews_value':'web.webPageDetails.subPageViews.Value',
        'payload_ACPExtensionEventData_xdm_web_webReferrer_URL':'web.webReferrer.URL',
        'payload_ACPExtensionEventData_xdm_web_siteRegion':'web.siteRegion',
        'payload_ACPExtensionEventData_xdm_web_language':'web.Language',
        'payload_ACPExtensionEventData_xdm_web_URL':'web.URL',
        'payload_ACPExtensionEventData_xdm_web_pageType':'web.PageType',
        'payload_ACPExtensionEventData_xdm_web_siteSection':'web.Sitesection',
        'payload_ACPExtensionEventData_xdm_web_name':'web.name',
        'payload_ACPExtensionEventData_xdm_web_subSection1':'web.subsection1',
        'payload_ACPExtensionEventData_xdm_web_subSection2': 'web.subsection2',
        'payload_ACPExtensionEventData_xdm_web_page':'web.page',
        'payload_ACPExtensionEventData_xdm_web_subPageName' :'web.subPageName',
        'payload_ACPExtensionEventData_xdm_web_subPageViews_value': 'web.subPageView',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_linkModuleName':'web.webInteraction.linkModuleName',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_name':'web.WebInteraction.name',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_type':'web.webInteraction.type',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_webInteraction_ctaClicks_value':'ctaClicks.value',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_linkClicks_value':'LinkClick.value',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_type':'webinteraction.type',
        'payload_ACPExtensionEventData_xdm__chipotle_loginStatus': '_chipotle.loginStatus',
        'payload_ACPExtensionEventData_xdm__chipotle_devicePlatform':'_chipotle.devicePlatform',
        'payload_ACPExtensionEventData_xdm__chipotle_orderNowClicks_value': '_chipotle.orderNowClick.value',
        'payload_ACPExtensionEventData_xdm__chipotle_errors_value':'_chipotle.errors.Value',
        'payload_ACPExtensionEventData_xdm__chipotle_errorName':'_chipotle.ErrorName',
        'payload_ACPExtensionEventData_xdm__chipotle_storeId':'_chipotle.storeID',
        'payload_ACPExtensionEventData_xdm__chipotle_deliveryAddressSubmitted_value':'_chipotle.deliveryAddressSubmitted.value',
        'payload_ACPExtensionEventData_xdm__chipotle_formNameDetails':'_chipotle.formNameDetails',
        'payload_ACPExtensionEventData_xdm__chipotle_loginType':'_chipotle.loginType',
        'payload_ACPExtensionEventData_xdm__chipotle_accountInteraction_value':'_chipotle.accountInteraction.value',
        'payload_ACPExtensionEventData_xdm__chipotle_registrationType':'_chipotle.registrationType',
        'payload_ACPExtensionEventData_xdm__chipotle_interactionName':'_chipotle.interactionName',
        'payload_ACPExtensionEventData_xdm__chipotle_addAddressDetails_value':'_chipotle.addAddressDetails.values',
        'payload_ACPExtensionEventData_xdm__chipotle_geoSearched':'_chipotle.geoSearched',
        'payload_ACPExtensionEventData_xdm__chipotle_deliveryAddressLookup_value':'_chipotle.deliveryAddresslookup.value',
        'payload_ACPExtensionEventData_xdm__chipotle_searchType':'_chipotle.searchType',
        'payload_ACPExtensionEventData_xdm__chipotle_cartEdits_value':'_chipotle.cardEdits.value',
        'payload_ACPExtensionEventData_xdm__chipotle_mealNameGiven_value':'_chipotle.mealNameGiven.value',
        'payload_ACPExtensionEventData_xdm__chipotle_orderDuplications_value':'_chipotle.orderDupications.value',
        'payload_ACPExtensionEventData_xdm__chipotle_orderDuplicates_value':'_chipotle.orderDuplicates.value',
        'payload_ACPExtensionEventData_xdm__chipotle_availablePoints':'_chipotle.avaiblePoints',
        'payload_ACPExtensionEventData_xdm__experience_analytics_customDimensions_eVars_eVar60':'eVars.evar60',
        'payload_ACPExtensionEventData_xdm__experience_analytics_customDimensions_eVars_eVar47':'evars.evar47',
        'payload_ACPExtensionEventData_xdm__experience_analytics_customDimensions_eVars_eVar9':'evars.eVar9',
        'payload_ACPExtensionEventData_xdm_marketing_internalCampaign':'marketing_internalCampain',
        'payload_ACPExtensionEventData_xdm_marketing_internalCampaignClicks_value':'internalCampainClicks.Value',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_webInteraction_ctaNameType':'webinteraction.ctaNameType',

        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteRegion':'web.webPageDetails.siteRegion',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_language': 'web.webPageDetails.Language',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageType' : '.web.webPageDetails.pageType',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_siteSection':'web.webPageDetails.siteSection',
        'payload_ACPExtensionEventData_xdm_web_webPageDetails_name': 'web.webPageDetails.name',
        #'web.webPageDetails.pageViews': 
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_previousPage':'mobile.mobilePageDetails.previousPage',
        'payload_ACPExtensionEventData_xdm_user_devicePlatform':'user.devicePlatform',
        'payload_ACPExtensionEventData_xdm_crewTipCount_value': "crewTipCount.value",
        #'payload_ACPExtensionEventData_xdm_crewTipCount_value',
        'payload_ACPExtensionEventData_xdm_commerce_order_orderID' :'commerce.order.orderID',
        'payload_ACPExtensionEventData_xdm_commerce_order_driverTipRevenue':'commerce.order.driverTipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_order_crewTipRevenue':'commerce.order.crewTipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_order_tipRevenue':'commerce.order.tipRevenue',
        'payload_ACPExtensionEventData_xdm_commerce_selectLocation_value':'commerce.selectLocation.value',
        'payload_ACPExtensionEventData_xdm_commerce_productListViews_value':'commerce.productListViews.value',
        'payload_ACPExtensionEventData_xdm_commerce_productListRemovals_value':'commerce.productListRemovals.value',
        'payload_ACPExtensionEventData_xdm_isDelivery':'isDelivery',
        'payload_ACPExtensionEventData_xdm_storeLocator_storeID':'storeLocator_storeID',
        'payload_ACPExtensionEventData_xdm_storeLocator_pickupLocation_value':'storeLocator_pickupLocation_value',
        'payload_ACPExtensionEventData_xdm_storeLocator_searchType':'storeLocator_searchType',
        'payload_ACPExtensionEventData_xdm_storeLocator_locationSelected_value':'storeLocator_locationSelected_value',
        'payload_ACPExtensionEventData_xdm_storeLocator_searchInitiated_value':'storeLocator_searchInitiated_value',
        'payload_ACPExtensionEventData_build.environment':'build.environment',
        'payload_ACPExtensionEventData_xdm_application_name':'application.name',


        'payload_ACPExtensionEventData_xdm_eventType':'EventType',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_pageViews_value':'web.webPageDetails.pageViews.value' ,
         'payload_ACPExtensionEventData_xdm_web_webInteraction_linkClicks_value':'web.webInteraction.linkClicks.value',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_name': 'web.webInteraction.name',
        'payload_ACPExtensionEventData_xdm_web_webInteraction_type':'web.webInteraction.type',
        'topThingsChoice': 'productListItems0.merchVars.topThingsChoice',
        'otherOptions': 'productListItems0.merchVars.otherOptions',
        'includedSides': 'productListItems0.merchVars.includedSides',
        'drinks':'productListItems0.merchVars.drinks',
        'riceChoice':'productListItems0.merchVars.riceChoice',
        'beansChoice':'productListItems0.merchVars.beansChoice',
        'productName':'productListItems0.merchVars.productName',
        'proteinChoice':'productListItems0.merchVars.proteinChoice',
        'SKU':'productListItems0.merchVars.SKU',
        'name':'productListItems0.merchVars.name',
        'quantity':'productListItems0.merchVars.quantity',
        'priceTotal':'productListItems0.merchVars.priceTotal',
        'payload_ACPExtensionEventData_xdm_commerce_productListAdds_value':'commerce.productListAdds.value',
        'payload_ACPExtensionEventData_xdm_commerce_productListOpens_value':'commerce.productListOpens.value',
        'payload_ACPExtensionEventData_xdm_mobile_mobilePageDetails_subPageName':'web.webPageDetails.subPageName',
        'payload_ACPExtensionEventData_xdm_shoppingCart_cartEdits_value':'shoppingCart.cartEdits.value',
        'payload_ACPExtensionEventData_xdm_commerce_checkouts_value': 'commerce.checkouts.value',
        'payload_ACPExtensionEventData_xdm_commerce_checkoutFunnelInteractions_value':'checkoutFunnelInteractions',
        'payload_ACPExtensionEventData_xdm_commerce_funnelName': 'commerce.funnelName',
    'payload_ACPExtensionEventData_xdm_commerce_retrievalType':'commerce.retrievalType',
    'payload_ACPExtensionEventData_xdm_commerce_purchases_value': 'commerce.purchases.value',
    'payload_ACPExtensionEventData_xdm_commerce_order_purchaseID': 'commerce.order.purchaseID',
    'payload_ACPExtensionEventData_xdm_commerce_order_taxRevenue':'commerce.order.taxRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_feeRevenue':'commerce.order.feeRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_donationRevenue':'commerce.order.donationRevenue',
    'payload_ACPExtensionEventData_xdm_commerce_order_groupOrderParticipants':'commerce.order.groupOrderParticipants',
    'payload_ACPExtensionEventData_xdm_commerce_order_paymentMethod':'commerce.order.paymentMethod',
    #'payload_currency-code':'/commerce/order/currencyCode',
    'payload_ACPExtensionEventData_xdm_commerce_order_pickupTime':'commerce.order.pickupTime',
    'payload_ACPExtensionEventData_xdm_commerce_checkoutType': 'commerce.checkoutType'

    },inplace=True)
    
    #json_Data['payload_ACPExtensionEventTimestamp'] = datetime.datetime.fromtimestamp(json_Data['payload_ACPExtensionEventTimestamp']).isoformat()
    
    
    #json_Data.drop_duplicates(inplace=True)
    #st.dataframe(json_Data)
    #st.write("before Transform")
    #st.dataframe(json_Data)
    n=json_Data[json_Data.duplicated()]
    #st.write(n.index)
    for i in n.index:
        json_Data.loc[i,'Notes']='Duplicate'
    for i in range(1,json_Data.shape[0]):
        if json_Data.loc[i,'Notes']=='Duplicate':
            json_Data.loc[i,'Kill or Consolidate?']=1
    #st.write(json_Data['Kill or Consolidate?'].sum())
    json_Data['Server Hits']=json_Data.index
    json_Data.loc[1,'Web']= '# of server calls found:'
    json_Data.loc[2,'Web']= json_Data.index.shape
    json_Data.loc[3,'Web']= ' # we can Kill or Consolidate:'
    json_Data.loc[4,'Web']= json_Data['Kill or Consolidate?'].sum()
    json_Data.loc[5,'Web']= '# that are Valid or for Discusson:'
    json_Data.loc[6,'Web']= json_Data.index.shape-json_Data['Kill or Consolidate?'].sum()
    json_Data.loc[7,'Web']= '% possible reduction:'
    json_Data.loc[8,'Web']= str(1-(json_Data.loc[6,'Web']/json_Data.index.shape))[1:7]+"%"
    kill_or_consolidate=(json_Data['Kill or Consolidate?'].sum())
    #st.write('kill',json_Data.loc[4,'Web'])
    #st.dataframe(json_Data)
    d=json_Data['Date'].iloc[0]
    st.write(d)
    json_Data = json_Data.T
    json_Data=json_Data.reset_index()
    #json_Data=json_Data.style.apply(style_specific_cell, axis=None)
    st.write("Filename: ", web_file.name)
    st.write("output file name",str(web_file.name)[:-5]+"_Output.csv")
    #st.write(os.sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'HasOffers_POSTCalls')))
    output = str(web_file.name)[:-5]+"_Output.xlsx"
    #csv= .to_csv(output)
    
    hit_count=json_Data.shape[1]-1
    valid =hit_count-kill_or_consolidate
    perc= 1-(valid/hit_count)
    output1 = BytesIO()
    with pd.ExcelWriter(output1, engine='xlsxwriter') as writer:
        format_r = writer.book.add_format({'bg_color':   '#FFF974',
                          'font_color': 'black',
                          'font_name':'Arial Black',
                          'bold':'True'})
        format_n = writer.book.add_format({'bg_color':   '#FFCCCB',
                          'font_color': 'black',
                          'bold':'True'})
        format_k= writer.book.add_format({
                          'font_color': 'black',
                          'font_name':'Arial Black',
                          'font_size':'11',
                          'bold':'True'})
        format_c= writer.book.add_format({
                           'bg_color':   '#FFCCCB',
                          'font_color': 'red',
                          'font_name':'Arial',
                          'font_size':'9'})
        format_g= writer.book.add_format({
                           'bg_color':   '#90EE90',
                          'font_color': 'green',
                          'font_name':'Arial',
                          'font_size':'9'})
        format_o= writer.book.add_format({
                           'bg_color':   '#FFD580',
                          'font_color': 'orange',
                          'font_name':'Arial',
                          'font_size':'9'})
        format_co= writer.book.add_format({
                           'bg_color':   '#FFF974',
                          'font_color': 'black',
                          'font_name':'Arial',
                          'font_size':'9'})
    # Write each dataframe to a different worksheet.
        json_Data.to_excel(writer,header=0,index=0,sheet_name='Sheet1')
        for column in json_Data:
            column_width = max(json_Data[column].astype(str).map(len).max(), len(str(column)))
            col_idx = json_Data.columns.get_loc(column)
            #st.write(col_idx)
            writer.sheets['Sheet1'].set_column(col_idx, col_idx, column_width)
            worksheet=writer.sheets['Sheet1']
            worksheet.write('C1',hit_count,format_r)
            worksheet.write('E1',kill_or_consolidate,format_r)
            worksheet.write('G1',valid,format_r)
            worksheet.write('I1',perc,format_r)
            worksheet.write('A1',web_file.name,format_r)
            from string import ascii_uppercase as alc
            n=alc
            
            for i in alc:
                worksheet.write_formula(i+str(4), '{IFERROR(FIND("kill",LOWER('+i+str(5)+')),0)+IFERROR(FIND("consolidate",LOWER('+i+str(5)+')),0)}')
                for j in n:
                    worksheet.write_formula(j+i+str(4), '{IFERROR(FIND("kill",LOWER('+j+i+str(5)+')),0)+IFERROR(FIND("consolidate",LOWER('+j+i+str(5)+')),0)}')
            worksheet.write_formula('E1', '{=SUM(B4:ZZ4)}',format_r)
            worksheet.write_formula('G1','{=(C1-E1)}',format_r)
            worksheet.write_formula('I1','{=1-(G1/C1)}',format_r)
            worksheet.write('A4','Kill or Consolidate?')
            from xlsxwriter.utility import xl_rowcol_to_cell
            worksheet.data_validation(4,1,4,hit_count, {'validate': 'list',
                                  'source': ['Consolidate', 'Kill (Duplicate)', 'Kill (Conditionally)',"Kill (Don't track)",'OK','OK w/value bug','Discuss']})
            worksheet.conditional_format('A1:A100', {'type': 'cell',
                                    'criteria': 'not equal to',
                                    'value':    1,
                                    'format':   format_k})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Kill (Duplicate)"',
                                    'format':   format_c})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Kill (Conditionally)"',
                                    'format':   format_c})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '''"Kill (Don't track)"''',
                                    'format':   format_c})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"OK"',
                                    'format':   format_g})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"OK w/value bug"',
                                    'format':   format_g})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Discuss"',
                                    'format':   format_o})
            worksheet.conditional_format('B5:ZZ5', {'type': 'cell',
                                    'criteria': 'equal to',
                                    'value':    '"Consolidate"',
                                    'format':   format_co})
            worksheet.freeze_panes(1, 1)



    writer.save()

   

    # Close the Pandas Excel writer and output the Excel file to the buffer
    
    #csv = convert_df(json_Data)
    
    
    st.download_button(
        label="Download data as Excel",
        data=output1,
        file_name= output,
        mime="application/vnd.ms-excel"
    ) 
    