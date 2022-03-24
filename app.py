import streamlit as st
#import plotly.express as px
import numpy as np
import pandas as pd
import base64
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
import plotly.graph_objects as go

# GCP API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

menu_items = {
	'Get help': 'https://www.youraedi.com',
	'Report a bug': 'https://www.youraedi.com',	
    'About':'''
	 
     ## Arkansas Cities and Counties Distribution Tool
	 Information presented in this tool was collected from the Arkansas Department of Finance and Administration (DFA) Local Distribution by North American Industry Classification System (NAICS). 
     This tool aims to make it easier for users to visualize and aggregate data provided by the Arkansas DFA.
	'''
}

st.set_page_config(
    page_title="Arkansas Distribution - Cities and Counties ",
    page_icon="https://arkansaseconomist.com/wp-content/uploads/2020/04/cropped-aedit_favicon_prep-32x32.png",
    layout="centered",#centered
    initial_sidebar_state="collapsed",
    menu_items=menu_items)
############End of Header


st.title('Arkansas Cities & Counties Distribution')
st.write("")
st.write("""
Information presented on this page was collected from the [Arkansas Department of Finance and Administration](https://www.ark.org/dfa/localtaxes/index.php) (DFA) Local Distribution by North American Industry Classification System (NAICS). 
The goal of this page is to make it easier for users to visualize and aggregate data openly and freely provided by the Arkansas DFA. Please refer to their webpage if you have any data reporting related questions.
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

############CACHE
if 'geo_ss' not in st.session_state:
    st.session_state.geo_ss=''   
if 'df_ss' not in st.session_state:
    st.session_state.df_ss=''

def ss_city():
    st.session_state.geo_ss=geography_selected    
    sql='SELECT * FROM `statetaxes-dfa.arkansas_taxes.city_tax_view` WHERE locationname ='+ "'"+ geography_selected+"'"
    df = pandas_gbq.read_gbq(sql, credentials=credentials)
    df.columns=['locationname','naics_code','dfa_naics_title','post_date','sales_date','total','rebate','tax_rate','taxable_sales','new_naics_code','new_naics_title','description','modified_indicator']
    #fix date
    df['post_date'] =  pd.to_datetime(df['post_date'], format='%m/%d/%Y')
    df['sales_date'] =  pd.to_datetime(df['sales_date'], format='%m/%d/%Y')
    st.session_state.df_ss=df
    return df

def ss_county():
    st.session_state.geo_ss=geography_selected    
    sql='SELECT * FROM `statetaxes-dfa.arkansas_taxes.county_tax_view` WHERE locationname ='+ "'"+ geography_selected+"'"
    df = pandas_gbq.read_gbq(sql, credentials=credentials)
    df.columns=['locationname','naics_code','dfa_naics_title','post_date','sales_date','total','rebate','tax_rate','taxable_sales','new_naics_code','new_naics_title','description','modified_indicator']
    #fix date
    df['post_date'] =  pd.to_datetime(df['post_date'], format='%m/%d/%Y')
    df['sales_date'] =  pd.to_datetime(df['sales_date'], format='%m/%d/%Y')    
    st.session_state.df_ss=df
    return df
 
###############CITY or COUNTY selection
##CITY RADIO SELECTED
if (geography_selection=='City'):
###################
#city_choices
    geography_chosen=city_choices
    ##########BUTTONS AND CONTENT
    #County Choice DROPDOWN
    geography_selected=st.selectbox('Select City',geography_chosen,on_change=ss_city)

    
    if geography_selected:
        #establishing new DB connection                           
        if st.session_state.geo_ss!=geography_selected:
            st.session_state.geo_ss=geography_selected            
            df=ss_city()
            #sql='SELECT * FROM `statetaxes-dfa.arkansas_taxes.city_tax_view` WHERE location_name ='+ "'"+ geography_selected+"'"
            #df = pandas_gbq.read_gbq(sql, credentials=credentials)
            #df.columns=['locationname','naics_code','dfa_naics_title','post_date','sales_date','total','rebate','tax_rate','taxable_sales','new_naics_code','new_naics_title','description','modified_indicator']
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
            upper_bound_year="2022"
            df_using['post_date']=pd.to_datetime(df_using['post_date'])
            df_using['post_year']=df_using['post_date'].dt.year
            df_using['post_month']=df_using['post_date'].dt.strftime('%b')
            df_using=df_using.sort_values(by=['locationname', 'naics_code','post_date']) #sorting data to take into account missing values
            #df_using=df[(df['post_date'].astype('str').str[0:4]).isin(years_selected)         
            
            #DESCRIPTION OF NAICS CODE
            with st.expander('Expand to see Description of NAICS Code '+ str(df_using['naics_code'].iloc[0]),expanded=False):
                df_using['description'].iloc[0]
                
            #NOTE FOR CHANGE
            if df_using['modified_indicator'].iloc[0]!=None:
                st.write(df_using['modified_indicator'].iloc[0])
        
        else: #use cached DB
            df=st.session_state.df_ss            
            naics_choices=sorted(df['dfa_naics_title'].unique())
            naics_selected=st.selectbox('Select NAICS',naics_choices)
            df_using=df[df['dfa_naics_title']==naics_selected]
            #YEARS
            years_choice=sorted((df_using['post_date'].astype('str').str[0:4]).unique())
            years_selected=st.multiselect('Select years (multiple selection allowed)',years_choice)
            ###### Data changes
            #sales year
            lower_bound_year="2017"
            upper_bound_year="2022"
            df_using['post_date']=pd.to_datetime(df_using['post_date'])
            df_using['post_year']=df_using['post_date'].dt.year
            df_using['post_month']=df_using['post_date'].dt.strftime('%b')
            df_using=df_using.sort_values(by=['locationname', 'naics_code','post_date']) #sorting data to take into account missing values
            #df_using=df[(df['post_date'].astype('str').str[0:4]).isin(years_selected)
            

            #DESCRIPTION OF NAICS CODE
            with st.expander('Expand to see Description of NAICS Code '+ str(df_using['naics_code'].iloc[0]),expanded=False):
                df_using['description'].iloc[0]
                
            #NOTE FOR CHANGE
            if df_using['modified_indicator'].iloc[0]!=None:
                st.write(df_using['modified_indicator'].iloc[0])

#### COUNTY RADIO SELECTED        
else:
###################
#county_choices
    geography_chosen=county_choices
    geography_selected=st.selectbox('Select County',geography_chosen,on_change=ss_county)

    if geography_selected:                            
        #establishing new DB connection                           
        if st.session_state.geo_ss!=geography_selected:
            st.session_state.geo_ss=geography_selected #updating session state            
            df=ss_county()
    
            ##########BUTTONS AND CONTENT
            #County Choice DROPDOWN
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
            upper_bound_year="2022"
            df_using['post_date']=pd.to_datetime(df_using['post_date'])
            df_using['post_year']=df_using['post_date'].dt.year
            df_using['post_month']=df_using['post_date'].dt.strftime('%b')
            df_using=df_using.sort_values(by=['locationname', 'naics_code','post_date']) #sorting data to take into account missing values
            #df_using=df[(df['post_date'].astype('str').str[0:4]).isin(years_selected)

            #DESCRIPTION OF NAICS CODE
            with st.expander('Expand to see Description of NAICS Code '+ str(df_using['naics_code'].iloc[0]),expanded=False):
                df_using['description'].iloc[0]

            #NOTE FOR CHANGE
            if df_using['modified_indicator'].iloc[0]!=None:
                st.write(df_using['modified_indicator'].iloc[0])
        
        else: #use cached DB connection                           
            df=st.session_state.df_ss  
            ##########BUTTONS AND CONTENT
            #County Choice DROPDOWN
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
            upper_bound_year="2022"
            df_using['post_date']=pd.to_datetime(df_using['post_date'])
            df_using['post_year']=df_using['post_date'].dt.year
            df_using['post_month']=df_using['post_date'].dt.strftime('%b')
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

# Note regarding DFA created categories
dfa_assigned_groups=['Other','Automobile','NAICS with Less Than 3 Businesses','Wholesale Vending and Other Unidentified Receipts','Other Adjustments']
if naics_selected in dfa_assigned_groups:
    st.write('Please note that this is not a NAICS code, this is a category assigned by Arkansas DFA.Code provide were created for illustration purposes only and aggregations are not representative of any NAICS categories.')


#creating saved values #ADDED
# # 4 Digit Graphs

end_date=str(df['post_date'].max())[0:10]
idx17to21=pd.DataFrame(data=pd.date_range(start='2017-1-1',end=end_date,freq='MS'), columns=['post_date'])
df_using=df_using.merge(idx17to21, how='right', on=['post_date'])
df_using['post_year']=df_using['post_date'].dt.year
df_using['post_month']=df_using['post_date'].dt.strftime('%b')

#4 Digit NAICS Graph
fig=go.Figure()
years=np.arange(2017,2023).astype(str)
color_graphs=['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#f781bf']
graph_colors=dict(zip(years, color_graphs))
for i in years:
    if str(i) in years_selected:                
        fig.add_scatter(
            name='Year'+i,
            x=df_using[df_using['post_year']==int(i)]['post_month'],            
            y=df_using[df_using['post_year']==int(i)]['total'].round(2),
            mode='markers+lines',
            marker=dict(color=graph_colors[i], size=2),
            showlegend=True
        )  

fig.update_layout(
    yaxis_title='Estimated Tax Distribution' ,
    xaxis_title='Month',
    yaxis_tickprefix = '$', 
    yaxis_tickformat = ',.',
    title='Estimated Tax Distribution for '+ geography_selected + " " +  naics_selected,
    autosize=True,
    #width=800,
    #height=600,    
    hovermode="x")
# Plot Chart
if years_selected:    
        st.plotly_chart(fig,use_container_with=True)




############################


#Button for Rebates

with st.expander('View Rebates Data'):
    fig=go.Figure()
    years=np.arange(2017,2023).astype(str)
    color_graphs=['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#f781bf']
    graph_colors=dict(zip(years, color_graphs))
    for i in years:
        if str(i) in years_selected:                
            fig.add_scatter(
                name='Year'+i,
                x=df_using[df_using['post_year']==int(i)]['post_month'],            
                y=df_using[df_using['post_year']==int(i)]['rebate'].round(2),
                mode='markers+lines',
                marker=dict(color=graph_colors[i], size=2),
                showlegend=True
            )  

    fig.update_layout(
        yaxis_title='Rebate Distribution' ,
        xaxis_title='Month',
        yaxis_tickprefix = '$', 
        yaxis_tickformat = ',.',
        title='Rebate Distribution for '+ geography_selected + " " +  naics_selected,
        autosize=True,
        #width=800,
        #height=600,    
        hovermode="x")
    # Plot Chart
    if years_selected:    
            st.plotly_chart(fig,use_container_with=True)

#Beginning of 3 Digit Analysis

st.subheader('Aggregated NAICS Information')
st.write('Information presented below can underrepresent local distribution due to data limitations related to collection, reporting, aggregation and privacy')

digit3=df[df['dfa_naics_title']==naics_selected]['naics_code'].iloc[0].astype(str)[0:3]
df.columns=['locationname','naics_code','dfa_naics_title','post_date','sales_date','total','rebate','tax_rate','taxable_sales','new_naics_code','new_naics_title','description','modified_indicator']
# 3 Digit naics names df_digit_temp
df_digit_temp=df[df['naics_code'].astype(str).str[0:3]==digit3]
df_digit_temp['post_date'] =  pd.to_datetime(df_digit_temp['post_date'], format='%m/%d/%Y')
df_digit_temp['sales_date'] =  pd.to_datetime(df_digit_temp['sales_date'], format='%m/%d/%Y')

df_digit_temp['post_date']=pd.to_datetime(df_digit_temp['post_date'])
df_digit_temp['post_year']=df_digit_temp['post_date'].dt.year
df_digit_temp['post_month']=df_digit_temp['post_date'].dt.strftime('%b')

#df=df.sort_values(by=['locationname', 'naics_code','post_date'])
#3 DIGIT

df_digit_temp=df_digit_temp.groupby(['post_date']).sum() #group by postdate
df_digit_temp.reset_index(inplace=True)
#df_digit_temp=
df_digit_temp=df_digit_temp.merge(idx17to21, how='right', on=['post_date'])
df_digit_temp['post_year']=df_digit_temp['post_date'].dt.year
df_digit_temp['post_month']=df_digit_temp['post_date'].dt.strftime('%b')

# 3 Digit NAICS Graph
fig=go.Figure()
years=np.arange(2017,2023).astype(str)
color_graphs=['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#f781bf']
graph_colors=dict(zip(years, color_graphs))
for i in years:
    if str(i) in years_selected:                
        fig.add_scatter(
            name='Year'+i,
            x=df_digit_temp[df_digit_temp['post_year']==int(i)]['post_month'],            
            y=df_digit_temp[df_digit_temp['post_year']==int(i)]['total'].round(2),
            mode='markers+lines',
            marker=dict(color=graph_colors[i], size=2),
            showlegend=True
        )  

fig.update_layout(
    yaxis_title='Estimated Tax Distribution' ,
    xaxis_title='Month',
    yaxis_tickprefix = '$', 
    yaxis_tickformat = ',.',
    title='Estimated Tax Distribution for '+ geography_selected + " - NAICS " +  digit3,
    autosize=True,
    #width=800,
    #height=600,    
    hovermode="x")
# Plot Chart
if years_selected:    
        st.plotly_chart(fig,use_container_with=True)



#Beginning of 2 Digit Analysis
digit2=digit3[0:2]
df_digit_temp2=df[df['naics_code'].astype(str).str[0:2]==digit2]
df_digit_temp2['post_date'] =  pd.to_datetime(df_digit_temp2['post_date'], format='%m/%d/%Y')
df_digit_temp2['sales_date'] =  pd.to_datetime(df_digit_temp2['sales_date'], format='%m/%d/%Y')
df_digit_temp2['post_date']=pd.to_datetime(df_digit_temp2['post_date'])
df_digit_temp2['post_year']=df_digit_temp2['post_date'].dt.year
df_digit_temp2['post_month']=df_digit_temp2['post_date'].dt.strftime('%b')
#df=df.sort_values(by=['locationname', 'naics_code','post_date'])
#3 DIGIT
end_date=str(df['post_date'].max())[0:10]
df_digit_temp2=df_digit_temp2.groupby(['post_date']).sum() #group by postdate
df_digit_temp2.reset_index(inplace=True)
idx17to21=pd.DataFrame(data=pd.date_range(start='2017-1-1',end=end_date,freq='MS'), columns=['post_date'])
#df_digit_temp2=
df_digit_temp2=df_digit_temp2.merge(idx17to21, how='right', on=['post_date'])
df_digit_temp2['post_year']=df_digit_temp2['post_date'].dt.year
df_digit_temp2['post_month']=df_digit_temp2['post_date'].dt.strftime('%b')

# 3 Digit NAICS Graph
fig=go.Figure()
years=np.arange(2017,2023).astype(str)
color_graphs=['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#f781bf']
graph_colors=dict(zip(years, color_graphs))
for i in years:
    if str(i) in years_selected:                
        fig.add_scatter(
            name='Year'+i,
            x=df_digit_temp2[df_digit_temp2['post_year']==int(i)]['post_month'],            
            y=df_digit_temp2[df_digit_temp2['post_year']==int(i)]['total'].round(2),
            mode='markers+lines',
            marker=dict(color=graph_colors[i], size=2),
            showlegend=True
        )  

fig.update_layout(
    yaxis_title='Estimated Tax Distribution' ,
    xaxis_title='Month',
    yaxis_tickprefix = '$', 
    yaxis_tickformat = ',.',
    title='Estimated Tax Distribution for '+ geography_selected + " - NAICS " +  digit2,
    autosize=True,
    #width=800,
    #height=600,    
    hovermode="x")
# Plot Chart
if years_selected:    
        st.plotly_chart(fig,use_container_with=True)





#DOWNLOAD FILE DEPRECATED
#df_download=df_using.copy()
#df_download.set_axis(['location name','naics code','naics title','post_date','sales/use date','tax distributed','rebate','tax rate','taxables sales','current naics code','new naics title','description','update note','sales year','sales month'],axis=1,inplace=True)
#df_download=df_download[['location name','naics code','post_date','tax distributed','tax rate','rebate','update note','description']]
#df_download['post_date']=df_download['post_date'].astype(str).str[0:10]
#CREATING A DOWNLOAD BUTTON 
#if st.button('Generate Download Link',help="Download all data available for the Geography and NAICS Code selected"):
#    csv = df_download.to_csv(index=False)
#    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
#    #grouping_file_name=geography_selected+"_"+geography_selection
#    file_name=geography_selected+"_"+geography_selection+"_TaxesDistribution.csv"
#    href = f'<a href="data:file/csv;base64,{b64}" download={file_name}>Download CSV File</a>'
#    st.markdown(href, unsafe_allow_html=True)

#create download button
data_download=df[['locationname','naics_code','dfa_naics_title','total','rebate','post_date']]
data_download=data_download.to_csv(index=False).encode('utf-8')
st.download_button(label="Download data for "+geography_selected +" as CSV",data=data_download,file_name=geography_selected+' tax_data'+'.csv',mime="text/csv")
del data_download


#CREATING THE FOOTNOTE AND INSERT LOGO
st.write("This webpage is brought to you by the [Arkansas Economic Development Institute](https://www.youraedi.com)")
col1,col2,col3=st.columns([2,1,2])
with col1:
    st.write(" ")
with col2:
    st.image("https://youraedi.com/wp-content/uploads/2020/08/aediLogoDownload.png",width=100,use_column_width=False,output_format='JPEG')
with col3:
    st.write(" ")



