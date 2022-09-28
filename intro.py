import streamlit as st
import json
import pandas as pd
import os
st.title("Hi welcome to saras")
data_file = st.file_uploader("Choose a file",type='json')



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
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out
if data_file == None:
    st.write("Please upload the File")
else:
    data = json.load(data_file)
    json_Data=pd.DataFrame([flatten_json(x) for x in data['events']])
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
        file_name=output,
        mime='text/csv',
    )