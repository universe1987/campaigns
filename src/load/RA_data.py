import pandas as pd
from datetime import datetime, timedelta
from stat_data import select_dist

def sam(df_all):
    df_sam0 = pd.read_excel("../../data/RA/Sam_2013_Mayoral_candidate_bios.xlsx")
    print df_sam0['CandID'].nunique()
    df_sam = df_sam0.groupby(['CandID'])['name'].count().reset_index().rename(columns={'name':'Sam'})
    df_sam = df_sam[~ df_sam['CandID'].isnull()]

    df_all = df_all.merge(df_sam, left_on='CandID', right_on='CandID', how ='outer')
    df_all.loc[:,'Sam'] = df_all['Sam'].astype(str)
    df_all.loc[:,'Sam'] = (df_all['Sam'].str.contains('(\d+)'))*1.0
    return df_all

def sam_source(df_all):
    dic_wiki = {'3461':'http://en.wikipedia.org/wiki/Kirk_Watson',
           '9396':'http://en.wikipedia.org/wiki/Carl_Stokes_(Baltimore)',
           '17346':'http://en.wikipedia.org/wiki/Catherine_E._Pugh',
           '21736':'http://en.wikipedia.org/wiki/Keiffer_J._Mitchell,_Jr.',
           '8973':'http://en.wikipedia.org/wiki/Kurt_Schmoke',
           '6757':"http://en.wikipedia.org/wiki/Martin_O'Malley'",
           '21742':'http://en.wikipedia.org/wiki/Mary_Pat_Clarke',
           '17344':'http://en.wikipedia.org/wiki/Sheila_Dixon#Career',
           '21729':'http://en.wikipedia.org/wiki/Stephanie_Rawlings-Blake',
           '138395':'http://en.wikipedia.org/wiki/Michael_F._Flaherty',
           '12593':'http://en.wikipedia.org/wiki/Raymond_Flynn',
           '28699':'http://en.wikipedia.org/wiki/Thomas_Menino',
           '100794':'http://en.wikipedia.org/wiki/Anthony_Foxx',
           '11001':'http://en.wikipedia.org/wiki/Pat_McCrory',
           '123':'http://en.wikipedia.org/wiki/Richard_Vinroot',
           '10867':'http://en.wikipedia.org/wiki/Michael_B._Coleman',
           '6236':'http://en.wikipedia.org/wiki/Laura_Miller',
           '372419':'https://en.wikipedia.org/wiki/Ron_Kirk#Post-mayoral_career',
           '18567':'http://en.wikipedia.org/wiki/Steve_Bartlett',
           '135592':'http://en.wikipedia.org/wiki/Tom_Leppert',
           '198243':'http://en.wikipedia.org/wiki/Dave_Bing#College',
           '81655':'http://en.wikipedia.org/wiki/Freman_Hendrix',
           '3707':'http://en.wikipedia.org/wiki/Kwame_Kilpatrick',
           '95990':'http://en.wikipedia.org/wiki/John_Cook_(mayor_of_El_Paso)',
           '9518':'http://en.wikipedia.org/wiki/Bart_Peterson',
           '166708':'http://en.wikipedia.org/wiki/Greg_Ballard',
           '143913':'http://en.wikipedia.org/wiki/Alvin_Brown',
           '6986':'http://en.wikipedia.org/wiki/John_Delaney_(Florida_politician)',
           '6982':'http://en.wikipedia.org/wiki/Nat_Glover',
           '2331':'http://en.wikipedia.org/wiki/Carol_Chumney',
           '159097':'http://en.wikipedia.org/wiki/Richard_Hackett',
           '1712':'https://en.wikipedia.org/wiki/Bob_Clement',
           '135612':'http://en.wikipedia.org/wiki/Karl_Dean',
           '19646':'http://en.wikipedia.org/wiki/Greg_Nickels#Political_career',
           '111116':'http://en.wikipedia.org/wiki/Cindy_Chavez',
           '111112':'http://en.wikipedia.org/wiki/Chuck_Reed',
           '19414':'http://en.wikipedia.org/wiki/Willie_Brown_(politician)#After_Mayorship',
           '8727':'http://en.wikipedia.org/wiki/Tom_Ammiano',
           '201052':'http://en.wikipedia.org/wiki/John_Avalos',
           '8728':'http://en.wikipedia.org/wiki/Gavin_Newsom',
           '19416':'http://en.wikipedia.org/wiki/Frank_Jordan#Personal',
           '261622':'http://en.wikipedia.org/wiki/Ed_Lee_(politician)',
           '25739':'http://en.wikipedia.org/wiki/Art_Agnos',
           '13979':'http://en.wikipedia.org/wiki/Susan_Golding',
           '88601':'http://en.wikipedia.org/wiki/Steve_Francis_(businessman)',
           '87396':'http://en.wikipedia.org/wiki/Jerry_Sanders_(politician)',
           '61131':'http://en.wikipedia.org/wiki/Donna_Frye',
           '13967':'http://en.wikipedia.org/wiki/Dick_Murphy',
           '92315':'http://en.wikipedia.org/wiki/Ron_Gonzales',
           '215061':'http://en.wikipedia.org/wiki/Michael_McGinn',
           '8608':'http://en.wikipedia.org/wiki/Norm_Rice',
           '19648':'http://en.wikipedia.org/wiki/Paul_Schell',
           '54581':'http://en.wikipedia.org/wiki/Adrian_Fenty',
           '3989':'http://en.wikipedia.org/wiki/Carol_Schwartz',
           '54564':'https://en.wikipedia.org/wiki/Linda_W._Cropp',
           '26431':'http://en.wikipedia.org/wiki/Marion_Barry#Early_life.2C_education.2C_and_civil_rights_activism',
           '32070':'http://en.wikipedia.org/wiki/Sharon_Pratt_Kelly',
           '60497':'http://en.wikipedia.org/wiki/Vincent_C._Gray',
           '86792':'http://en.wikipedia.org/wiki/Phil_Gordon_(politician)',
           '105991':'http://en.wikipedia.org/wiki/Tom_Knox',
           '4573':'https://en.wikipedia.org/wiki/Sam_Katz_(Philadelphia)',
           '47026':'http://en.wikipedia.org/wiki/Michael_Nutter',
           '43012':'http://en.wikipedia.org/wiki/Lucien_E._Blackwell',
           '4572':'http://en.wikipedia.org/wiki/John_F._Street',
           '16':'http://en.wikipedia.org/wiki/Ed_Rendell#Personal_life',
    }
    dic_linkedin = {'6236':'http://www.linkedin.com/pub/laura-miller/1b/a82/546',
                    '135592':'http://www.linkedin.com/in/tomleppert',
                    '92315':'http://www.linkedin.com/pub/ron-gonzales/5/b45/106',
                    '13979':'http://www.linkedin.com/pub/susan-golding/27/58a/9a1',
                    '86792':'http://www.linkedin.com/pub/phil-gordon/38/6a4/257',
                    '105991':'http://www.linkedin.com/pub/thomas-knox/a/682/5b5',
                    '15921':'http://www.linkedin.com/pub/al-taubenberger/23/482/a33',
                    '4597':'http://www.linkedin.com/in/rudygiuliani',
                    }

    dic_others =   {'1075':['http://rush.house.gov/'],
                    '6427':['http://ajwright.com/documents/Rev.PaulJakesCV.pdf'],
                    '20022':['http://library.csu.edu/collections/pincham/history/'],
                    '5894':['http://votesmart.org/candidate/biography/8018/sylvester-turner#.UelFN2T72XQ','http://ballotpedia.org/wiki/index.php/Sylvester_Turner'],
                    '8270':['http://www.nytimes.com/2005/08/10/nyregion/10biobox.html?_r=0','http://iarchives.nysed.gov/xtf/view?docId=03-01_03-04a_03-04b_03-05_04-53.xml;query=;brand=default'],
                    '8727':['http://www.asmdc.org/members/a17/'],
                    '47026':['http://www.thehistorymakers.com/biography/hon-michael-nutter'],
                    '16':['http://www.ballardspahr.com/eventsnews/pressreleases/2011-01-24_edwardrendellreturnstoballardspahr.aspx'],
                    '15921':['https://cityroom.blogs.nytimes.com/2009/05/27/emboldened-thompson-presses-his-mayoral-bid/?scp=4&sq=2009%20mayor%20race&st=cse'],
                    '8073':['http://web.archive.org/web/20010731182537/','http://www.nypn.org/htm/resources/ruth-messinger.html','http://web.archive.org/web/19970121104613/','http://ruth97.org/',],
                    '4597':['http://www.biography.com/people/rudolph-giuliani-9312674','http://www.nyc.gov/html/records/rwg/html/bio.html'],
                    '4634':['http://www.nyc.gov/portal/site/nycgov/menuitem.e985cf5219821bc3f7393cd401c789a0','http://www.businessinsider.com/michael-bloomberg-biography-2012-7?op=1','http://www.mikebloomberg.com/index.cfm?objectid=e689d66f-96fd-e9f6-b1af64b8dae78a69'],
                    '6623':['http://www.huffingtonpost.com/author/mark-green']
                  }

    #http://en.wikipedia.org/wiki/Walter_Moore_(politician)
    #https://en.wikipedia.org/wiki/Greg_Lashutka
    #http://en.wikipedia.org/wiki/Bob_Lanier_(politician)
    #http://www.houstontx.gov/mayor/bio.html
    #http://en.wikipedia.org/wiki/Matt_Gonzalez#2008_presidential_race
    #'2813':

    df_all['Wikipedia'] = df_all['Linkedin'] = df_all['Others1'] = df_all['Others2'] = df_all['Others3'] = df_all['Others4'] = ""
    df_all['CandIDs'] = df_all['CandID'].astype(float)
    for key, value in dic_wiki.iteritems():
        df_all.loc[df_all['CandIDs']==float(key),'Wikipedia']=value
    for key, value in dic_linkedin.iteritems():
        df_all.loc[df_all['CandIDs']==float(key),'Linkedin']=value
    for key, value in dic_others.iteritems():
        h = len(value)
        for i in range(h):
            df_all.loc[df_all['CandIDs']==float(key),'Others{}'.format(i+1)] = value[i]
    return df_all

def RA_name_list(df_all,output):
    df_name_RA = df_all[['Name','City','CityID','CandID','Sam','winner ever','winner_key ever',
                         'follower ever','follower_key ever','First Try','Last Try',
                         'Wikipedia','Linkedin','Others1', 'Others2', 'Others3', 'Others4']]

    df_name_RA = df_name_RA.groupby(['City','CityID','CandID','Sam','winner ever','winner_key ever',
                                     'follower ever','follower_key ever','First Try','Last Try',
                                     'Wikipedia', 'Linkedin', 'Others1', 'Others2', 'Others3', 'Others4'])['Name'].min().reset_index()

    df_name_RA.loc[:, 'CityID'] = df_name_RA['CityID'].astype(float)
    df_name_RA = df_name_RA.sort_values(['CityID', 'CandID'], ascending=True)
    df_name_RA['Web'] = df_name_RA['CandID'].astype(int).astype(str)
    df_name_RA.loc[:, 'Web'] = 'http://www.ourcampaigns.com/CandidateDetail.html?CandidateID=' + df_name_RA['Web']
    print df_name_RA['CandID'].nunique()

    df_name_RA.loc[:,'CityID'] = df_name_RA['CityID'].astype(float)
    df_name_RA = df_name_RA.sort_values(['CityID','First Try'],ascending=[True, False])
    print 'sam marked:', df_name_RA['Sam'].sum()

    df_name_RA = df_name_RA[['Name','CandID','City','CityID','Wikipedia','Linkedin','Others1','Others2','Others3','Others4','Web']]
    df_name_RA = df_name_RA.reset_index().drop('index',1)
    df_name_RA.to_csv("../../data/RA/{}.csv".format(output))

    return df_name_RA

if __name__ == '__main__':
    df_name = pd.read_pickle("../../data/pdata/df_all_CityID.pkl")
    df_name = sam(df_name)
    df_name = sam_source(df_name)

    # First Stage:
    df_task1 = df_name[df_name['winner ever']+df_name['follower ever']+df_name['winner_key ever']+df_name['follower_key ever']>0]
    df_task1 = select_dist(df_task1, [['CityID', 150]], [['Last Try', datetime(1970, 1, 1)]])
    RA_name_list(df_task1,'Jeremey_08_02_2017')
    # Jeremey worked hard over a week...
    df_fruit1 = pd.read_csv("../../data/RA/Jeremey_08_07_2017.csv")
    for x in ['Wikipedia','Linkedin']:
        df_fruit1.loc[df_fruit1[x].isnull(),x] = 'Not Available'

    # Second Stage:
    df_task2 = select_dist(df_task1, [['CityID', 150]], [['Last Try', datetime(1970, 1, 1)]])

    #RA_name_list(df_task2,)


