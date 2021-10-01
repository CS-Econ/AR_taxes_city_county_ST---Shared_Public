

import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import pymysql
import base64
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

# Create GCP API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


###########Header
#hide_streamlit_style = """
#            <style>
#            #MainMenu {visibility: hidden;}
#            footer {visibility: hidden;}
#            page_title="Arkansas Taxable Sales - Counties and Cities "{visibility:True;}
#            </style>
#st.markdown(hide_streamlit_style, unsafe_allow_html=True)
st.set_page_config(
    page_title="Arkansas Distribution - Cities and Counties ",
    page_icon="https://arkansaseconomist.com/wp-content/uploads/2020/04/cropped-aedit_favicon_prep-32x32.png",
    layout="centered",#centered
    initial_sidebar_state="collapsed")
############End of Header


st.title('Arkansas Cities & Counties Distribution')
st.write("")
st.write("""
Information presented on this page was collected from the [Arkansas Department of Finance and Administration] (https://www.ark.org/dfa/localtaxes/index.php) (DFA) Local Distribution by North American Industry Classification System (NAICS). 
The goal of this page is to make it easier for users to visualize and aggregate data openly and freely provided by the Arkansas DFA. Please refer to their webpage if you have any data related questions.
""")


########DICTIONARIES

st.header('Visualize Local Tax Distribution within your area')
geography_selection=st.radio("Select Geography",("City","County"))

######################database connection 
#CACHING County and City Data
@st.cache(persist=True)
def caching_locations():
    city_choices=['Stuttgart', 'Dewitt', 'Humphrey', 'Gillett', 'Almyra', 'St. Charles', 'Crossett', 'Fountain Hill', 'Hamburg', 'Parkdale', 'Portland', 'Wilmot', 'Mountain Home', 'Cotter', 'Gassville', 'Norfork', 'Lakeview', 'Big Flat', 'Salesville', 'Briarcliff', 'Siloam Springs', 'Rogers', 'Bentonville', 'Bethel Heights', 'Decatur', 'Gentry', 'Gravette', 'Lowell', 'Centerton', 'Pea Ridge', 'Cave  Springs', 'Sulphur Springs', 'Avoca', 'Garfield', 'Highfill', 'Little Flock', 'Springtown', 'Bella Vista', 'Alpena', 'Lead Hill', 'Harrison', 'Diamond City', 'Hermitage', 'Warren', 'Hampton', 'Thornton', 'Eureka Springs', 'Berryville', 'Green Forest', 'Oak Grove', 'Lake Village', 'Eudora', 'Dermott', 'Caddo Valley', 'Arkadelphia', 'Gurdon', 'Amity', 'Gum Springs', 'Corning', 'Rector', 'Piggott', 'Greers Ferry', 'Heber Springs', 'Quitman', 'Rison', 'Kingsland', 'Magnolia', 'Taylor', 'Morrilton', 'Menifee', 'Oppelo', 'Plumerville', 'Bay', 'Bono', 'Brookland', 'Caraway', 'Cash', 'Lake City', 'Monette', 'Jonesboro', 'Alma', 'Van Buren', 'Mulberry', 'Mountainburg', 'Kibler', 'Dyer', 'Rudy', 'Cedarville', 'Marion', 'West Memphis', 'Earle', 'Crawfordsville', 'Gilmore', 'Jennette', 'Sunset', 'Turrell', 'Anthonyville', 'Cherry Valley', 'Parkin', 'Wynne', 'Sparkman', 'Fordyce', 'Mcgehee', 'Dumas', 'Monticello', 'Conway', 'Mayflower', 'Greenbrier', 'Vilonia', 'Damascus', 'Guy', 'Branch', 'Wiederkehr Village', 'Altus', 'Charleston', 'Ozark', 'Mammoth Spring', 'Salem', 'Viola', 'Hot Springs', 'Sheridan', 'Marmaduke', 'Oak Grove Heights', 'Paragould', 'Hope', 'Blevins', 'Mccaskill', 'Patmos', 'Washington', 'Malvern', 'Perla', 'Rockport', 'Nashville', 'Dierks', 'Mineral Springs', 'Batesville', 'Moorefield', 'Pleasant Plains', 'Horseshoe Bend', 'Melbourne', 'Calico Rock', 'Pineville', 'Franklin', 'Oxford', 'Guion', 'Newport', 'Tuckerman', 'Beedeville', 'Diaz', 'Swifton', 'Pine Bluff', 'Wabbaseka', 'White Hall', 'Redfield', 'Altheimer', 'Sherrill', 'Clarksville', 'Coal Hill', 'Lamar', 'Bradley', 'Stamps', 'Lewisville', 'Walnut Ridge', 'Black Rock', 'Hoxie', 'Imboden', 'Portia', 'Ravenden', 'Marianna', 'Moro', 'Star City', 'Gould', 'Grady', 'Ashdown', 'Wilton', 'Foreman', 'Blue Mountain', 'Magazine', 'Morrison Bluff', 'Paris', 'Scranton', 'Subiaco', 'Booneville', 'Austin', 'Carlisle', 'England', 'Keo', 'Lonoke', 'Ward', 'Cabot', 'Huntsville', 'St. Paul', 'Bull Shoals', 'Flippin', 'Pyatt', 'Summit', 'Yellville', 'Garland', 'Fouke', 'Texarkana', 'Osceola', 'Keiser', 'Blytheville', 'Gosnell', 'Joiner', 'Leachville', 'Luxora', 'Manila', 'Wilson', 'Etowah', 'Clarendon', 'Brinkley', 'Holly Grove', 'Roe', 'Norman', 'Mount Ida', 'Prescott', 'Jasper', 'Western Grove', 'Camden', 'Stephens', 'East Camden', 'Bearden', 'Chidester', 'Perryville', 'Marvell', 'Helena-West Helena', 'Delight', 'Glenwood', 'Murfreesboro', 'Lepanto', 'Harrisburg', 'Marked Tree', 'Trumann', 'Tyronza', 'Weiner', 'Waldenburg', 'Mena', 'Cove', 'Hatfield', 'Vandervoort', 'Wickes', 'Russellville', 'Atkins', 'Dover', 'Hector', 'Pottsville', 'Hazen', 'Des Arc', 'Devalls Bluff', 'North Little Rock', 'Alexander', 'Jacksonville', 'Little Rock', 'Maumelle', 'Sherwood', 'Maynard', 'Pocahontas', 'Bryant', 'Shannon Hills', 'Benton', 'Bauxite', 'Haskell', 'Waldron', 'Gilbert', 'Leslie', 'Marshall', 'Fort Smith', 'Huntington', 'Mansfield', 'Barling', 'Greenwood', 'Bonanza', 'Hackett', 'Hartford', 'De Queen', 'Gillham', 'Horatio', 'Lockesburg', 'Hardy', 'Ash Flat', 'Cave City', 'Evening Shade', 'Cherokee Village', 'Highland', 'Hughes', 'Forrest City', 'Wheatley', 'Palestine', 'Madison', 'Widener', 'Mountain View', 'El Dorado', 'Junction City', 'Strong', 'Shirley', 'Clinton', 'Fairfield Bay', 'Elkins', 'Elm Springs', 'Goshen', 'Greenland', 'Johnson', 'Prairie Grove', 'Springdale', 'Tontitown', 'West Fork', 'Winslow', 'Fayetteville', 'Lincoln', 'Farmington', 'Rose Bud', 'Beebe', 'Bradford', 'Higginson', 'Judsonia', 'Mcrae', 'Pangburn', 'Searcy', 'Bald Knob', 'Cotton Plant', 'Augusta', 'Mccrory', 'Patterson', 'Plainview', 'Dardanelle', 'Ola', 'Danville', 'Belleville', 'Havana']
    city_choices=sorted(city_choices)
    county_choices=['Arkansas', 'Ashley', 'Baxter', 'Benton', 'Boone', 'Bradley', 'Calhoun', 'Carroll', 'Chicot', 'Clark', 'Clay', 'Cleburne', 'Cleveland', 'Columbia', 'Conway', 'Craighead', 'Crawford', 'Crittenden', 'Cross', 'Dallas', 'Desha', 'Drew', 'Faulkner', 'Franklin', 'Fulton', 'Garland', 'Grant', 'Greene', 'Hempstead', 'Hot Spring', 'Howard', 'Independence', 'Izard', 'Jackson', 'Jefferson', 'Johnson', 'Lafayette', 'Lawrence', 'Lee', 'Lincoln', 'Little River', 'Logan', 'Lonoke', 'Madison', 'Marion', 'Miller', 'Mississippi', 'Monroe', 'Montgomery', 'Nevada', 'Newton', 'Ouachita', 'Perry', 'Phillips', 'Pike', 'Poinsett', 'Polk', 'Pope', 'Prairie', 'Pulaski', 'Randolph', 'Saline', 'Scott', 'Searcy', 'Sebastian', 'Sevier', 'Sharp', 'St. Francis', 'Stone', 'Union', 'Van Buren', 'Washington', 'White', 'Woodruff', 'Yell']
    county_choices=sorted(county_choices)
    
    #city_choices=sorted(city_choices)
    return city_choices, county_choices
city_choices=caching_locations()[0]
county_choices=caching_locations()[1]

#@st.cache(ttl=600)
###############CITY or COUNTY selection
if (geography_selection=='City'):
###################
#city_choices
    geography_chosen=city_choices
    ##########BUTTONS AND CONTENT
    #County Choice DROPDOWN
    geography_selected=st.selectbox('Select City',geography_chosen)
    sql='SELECT * FROM `statetaxes-dfa.arkansas_taxes.city_tax_view` WHERE location_name ='+ "'"+ geography_selected+"'"
    df = pandas_gbq.read_gbq(sql, credentials=credentials)
    df.columns=['locationname','naics_code','dfa_naics_title','post_date','sales_date','total','rebate','tax_rate','taxable_sales','new_naics_code','new_naics_title','description','modified_indicator']
    #NAICS Choice DROPDOWN
    naics_choices=sorted(df['dfa_naics_title'].unique())
    naics_selected=st.selectbox('Select NAICS',naics_choices)
    df_using=df[df['dfa_naics_title']==naics_selected]
    #YEARS
    years_choice=sorted((df_using['post_date'].astype('str').str[0:4]).unique())
    years_selected=st.multiselect('Select years (multiple selection allowed)',years_choice)
    ###### Data changes
    #sales year
    lower_bound_year="2018"
    upper_bound_year="2020"
    df_using['post_date']=pd.to_datetime(df_using['post_date'])
    df_using['sales_year']=df_using['post_date'].dt.year
    df_using['sales_month']=df_using['post_date'].dt.strftime('%b')
    df_using=df_using.sort_values(by=['locationname', 'naics_code','post_date']) #sorting data to take into account missing values
    #df_using=df[(df['post_date'].astype('str').str[0:4]).isin(years_selected)
    

    #DESCRIPTION OF NAICS CODE
    with st.expander('Expand to see Description of NAICS Code '+ str(df_using['naics_code'].iloc[0]),expanded=False):
        df_using['description'].iloc[0]
        
    #NOTE FOR CHANGE
    if df_using['modified_indicator'].iloc[0]!=None:
        st.write(df_using['modified_indicator'].iloc[0])
        
else:
###################
#county_choices
    geography_chosen=county_choices
    ##########BUTTONS AND CONTENT
    #County Choice DROPDOWN
    geography_selected=st.selectbox('Select County',geography_chosen)
    sql='SELECT * FROM `statetaxes-dfa.arkansas_taxes.county_tax_view` WHERE location_name ='+ "'"+ geography_selected+"'"
    df = pandas_gbq.read_gbq(sql, credentials=credentials)
    df.columns=['locationname','naics_code','dfa_naics_title','post_date','sales_date','total','rebate','tax_rate','taxable_sales','new_naics_code','new_naics_title','description','modified_indicator']
    #NAICS Choice DROPDOWN
    naics_choices=sorted(df['dfa_naics_title'].unique())
    naics_selected=st.selectbox('Select NAICS',naics_choices)
    df_using=df[df['dfa_naics_title']==naics_selected]
    #YEARS
    years_choice=sorted((df_using['post_date'].astype('str').str[0:4]).unique())
    years_selected=st.multiselect('Select years (multiple selection allowed)',years_choice)
    ###### Data changes
    #sales year
    lower_bound_year="2017"
    upper_bound_year="2021"
    df_using['post_date']=pd.to_datetime(df_using['post_date'])
    df_using['sales_year']=df_using['post_date'].dt.year
    df_using['sales_month']=df_using['post_date'].dt.strftime('%b')
    df_using=df_using.sort_values(by=['locationname', 'naics_code','post_date']) #sorting data to take into account missing values
    #df_using=df[(df['post_date'].astype('str').str[0:4]).isin(years_selected)

    #DESCRIPTION OF NAICS CODE
    with st.expander('Expand to see Description of NAICS Code '+ str(df_using['naics_code'].iloc[0]),expanded=False):
        df_using['description'].iloc[0]

    #NOTE FOR CHANGE
    if df_using['modified_indicator'].iloc[0]!=None:
        st.write(df_using['modified_indicator'].iloc[0])


#OUTPUT 
#Adding the min&max for (years_selected) allows the chart present values from Jan to Dec
#if years_selected:
#    lower_bound_year=str(min(years_selected))
#    upper_bound_year=str(max(years_selected))

#creating saved values
if int(2017) in df_using['sales_year'].unique():
    df_using17=df_using[df_using['sales_year']==2017]
    #taking care of the missing months
    idx17=pd.DataFrame(data=pd.date_range(start='2017-1-1',end='2017-12-1',freq='MS'), columns=['post_date'])
    df_using17=df_using17.merge(idx17, how='right', on=['post_date'])
    df_using17['local_code']=geography_selected
    df_using17['naics_code']=naics_selected
    df_using17['sales_month']=df_using17['post_date'].dt.strftime('%b')    
if int(2018) in df_using['sales_year'].unique():
    df_using18=df_using[df_using['sales_year']==2018]
    idx18=pd.DataFrame(data=pd.date_range(start='2018-1-1',end='2018-12-1',freq='MS'), columns=['post_date'])
    df_using18=df_using18.merge(idx18, how='right', on=['post_date'])
    df_using18['local_code']=geography_selected
    df_using18['naics_code']=naics_selected
    df_using18['sales_month']=df_using18['post_date'].dt.strftime('%b') 
if int(2019) in df_using['sales_year'].unique():
    df_using19=df_using[df_using['sales_year']==2019]
    idx19=pd.DataFrame(data=pd.date_range(start='2019-1-1',end='2019-12-1',freq='MS'), columns=['post_date'])
    df_using19=df_using19.merge(idx19, how='right', on=['post_date'])
    df_using19['local_code']=geography_selected
    df_using19['naics_code']=naics_selected
    df_using19['sales_month']=df_using19['post_date'].dt.strftime('%b') 
if int(2020) in df_using['sales_year'].unique():
    df_using20=df_using[df_using['sales_year']==2020]
    idx20=pd.DataFrame(data=pd.date_range(start='2020-1-1',end='2020-12-1',freq='MS'), columns=['post_date'])
    df_using20=df_using20.merge(idx20, how='right', on=['post_date'])
    df_using20['local_code']=geography_selected
    df_using20['naics_code']=naics_selected
    df_using20['sales_month']=df_using20['post_date'].dt.strftime('%b') 
if int(2021) in df_using['sales_year'].unique():
    df_using21=df_using[df_using['sales_year']==2021]
    idx21=pd.DataFrame(data=pd.date_range(start='2021-1-1',end='2021-12-1',freq='MS'), columns=['post_date'])
    df_using21=df_using21.merge(idx21, how='right', on=['post_date'])
    df_using21['local_code']=geography_selected
    df_using21['naics_code']=naics_selected
    df_using21['sales_month']=df_using21['post_date'].dt.strftime('%b') 

#fig = px.line(df_using, x="post_date", y="total",color='sales_year',range_x=[lower_bound_year+'-01-01',upper_bound_year+'-12-01'])
#fig.update_xaxes(title_text='Month')
#fig.update_yaxes(title_text='Taxable Sales')
#fig.date[0].update(mode='markers+lines')

import plotly.graph_objects as go
#fig=Figure(data=df_using, x='sales_month',y='total')
# melt data to provide the data structure mentioned earlier

#fig = px.line(df_using, x="sales_month", y="total",color='sales_year')
#fig.add_scatter(x=df_using['sales_month'], y=df_using['total']) # Not what is desired - need a line 
#df1 = df_using.melt(id_vars=['sales_month','total'], var_name='local_code')
#st.write(df1)
#fig=px.line(df_using, x="sales_month", y="total",)
#fig.add_scatter(x=df_using['sales_month'], y=df_using['total'])
#fig.add_scatter(x=df_using['sales_month'], y=df_using['total'])
#Creating Figure
fig=go.Figure()
if '2017' in years_selected:
        fig.add_scatter(
            name='Year 2017',
            x=df_using17['sales_month'],
            y=df_using17['total'],
            mode='markers+lines',
            marker=dict(color='red', size=2),
            showlegend=True
        )
if '2018' in years_selected:
    fig.add_scatter(
            name='Year 2018',
            x=df_using18['sales_month'],
            y=df_using18['total'],
            mode='markers+lines',
            marker=dict(color='blue', size=2),
            showlegend=True
        )
if '2019' in years_selected:
    fig.add_scatter(
            name='Year 2019',
            x=df_using19['sales_month'],
            y=df_using19['total'],
            mode='markers+lines',
            marker=dict(color='green', size=2),
            showlegend=True
        )
if '2020' in years_selected:
    fig.add_scatter(
            name='Year 2020',
            x=df_using20['sales_month'],
            y=df_using20['total'],
            mode='markers+lines',
            marker=dict(color='purple', size=2),
            showlegend=True
        )
if '2021' in years_selected:
    fig.add_scatter(
            name='Year 2021',
            x=df_using21['sales_month'],
            y=df_using21['total'],
            mode='markers+lines',
            marker=dict(color='orange', size=2),
            showlegend=True
        )
fig.update_layout(
    yaxis_title='Taxable Distribution',
    xaxis_title='Month',
    yaxis_tickprefix = '$', 
    yaxis_tickformat = ',.',
    title='Tax Distribution for '+ geography_selected,
    autosize=True,
    #width=800,
    #height=600,
    hovermode="x")
# Plot Chart
if years_selected:    
        st.plotly_chart(fig,use_container_with=True)



#Button for Rebates

with st.expander('View Rebates Data'):
    fig=go.Figure()
    if '2017' in years_selected:
            fig.add_scatter(
                name='Year 2017',
                x=df_using17['sales_month'],
                y=df_using17['rebate'],
                mode='markers+lines',
                marker=dict(color='red', size=2),
                showlegend=True
            )
    if '2018' in years_selected:
        fig.add_scatter(
                name='Year 2018',
                x=df_using18['sales_month'],
                y=df_using18['rebate'],
                mode='markers+lines',
                marker=dict(color='blue', size=2),
                showlegend=True
            )
    if '2019' in years_selected:
        fig.add_scatter(
                name='Year 2019',
                x=df_using19['sales_month'],
                y=df_using19['rebate'],
                mode='markers+lines',
                marker=dict(color='green', size=2),
                showlegend=True
            )
    if '2020' in years_selected:
        fig.add_scatter(
                name='Year 2020',
                x=df_using20['sales_month'],
                y=df_using20['rebate'],
                mode='markers+lines',
                marker=dict(color='purple', size=2),
                showlegend=True
            )
    if '2021' in years_selected:
        fig.add_scatter(
                name='Year 2021',
                x=df_using21['sales_month'],
                y=df_using21['rebate'],
                mode='markers+lines',
                marker=dict(color='orange', size=2),
                showlegend=True
            )
    fig.update_layout(
        yaxis_title='Rebate Distribution',
        xaxis_title='Month',
        yaxis_tickprefix = '$', 
        yaxis_tickformat = ',.',
        title='Rebate Distribution for '+ geography_selected,
        autosize=True,
        #width=800,
        #height=600,
        hovermode="x")
    # Plot Chart
    if years_selected:
        st.plotly_chart(fig,use_container_with=True)




#CREATING A DOWNLOAD BUTTON 
df_download=df_using.copy()
df_download.set_axis(['location name','naics code','naics title','post_date','sales/use date','tax distributed','rebate','tax rate','taxables sales','current naics code','new naics title','description','update note','sales year','sales month'],axis=1,inplace=True)
df_download=df_download[['location name','naics code','post_date','tax distributed','tax rate','rebate','update note','description']]
df_download['post_date']=df_download['post_date'].astype(str).str[0:10]
#DOWNLOAD FILE
if st.button('Generate Download Link',help="Download all data available for the Geography and NAICS Code selected"):
    csv = df_download.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    #grouping_file_name=geography_selected+"_"+geography_selection
    file_name=geography_selected+"_"+geography_selection+"_TaxesDistribution.csv"
    href = f'<a href="data:file/csv;base64,{b64}" download={file_name}>Download CSV File</a>'
    st.markdown(href, unsafe_allow_html=True)

#CREATING THE FOOTNOTE AND INSERT LOGO
st.write("This webpage is brought to you by the [Arkansas Economic Development Institute](https:www.youraedi.com)")
col1,col2,col3=st.columns([2,1,2])
with col1:
    st.write(" ")
with col2:
    st.image("https://youraedi.com/wp-content/uploads/2020/08/aediLogoDownload.png",width=100,use_column_width=False,output_format='JPEG')
with col3:
    st.write(" ")


