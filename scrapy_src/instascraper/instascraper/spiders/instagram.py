#imports
import os
import json
import scrapy
from datetime import datetime
from kafka import KafkaProducer
from urllib.parse import urlencode

#kafka config
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))
#send data to kafka
def send_to_kafka(input:dict):
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
    producer.send('rawdata', input)
#account that woud be scrape
user_accounts = ["davidderuiter","reqdyy","yeezy.visions",
"noah_gesser","noahhaynes88","b1essah","oliver.skipp",
"oliverbenoitt","oliverjburke","n.o_369_","elijahtatis",
"mxelijah","iamelijahfisher","pcdubss","williamyyellow",
"boldyjames","aguerobenja19","ben_white6","strombeck_","lucasgagliasso"]
#cookies that send with every request
cookies={
            'ig_nrcb':'1',
            'mid':'YWMiUgALAAEr8PEAL_BE1wvUih3K	',
            'ig_did':'99F6FF7E-C3B4-471F-86EB-3FDF54E7F858',
            'shbid':'"14492\05444040163983\0541665424736:01f7a4b3719f78c62d5bfaa8fb5b61af00e5c68a0dc5497fede250664ce1674aa89a11f5"',
            'shbts':'"1633888736\05444040163983\0541665424736:01f7aa4b36cc29546904d4550087536941c07ffb5843fca8df97b1eedc6471f0d4fc29a7"',
            'csrftoken':'21l7iyZ7dDUti3dGaAiWsH3rflpqB7FK',
            'ds_user_id':'44040163983',
            'sessionid':'44040163983%3AFFliM3E38UmH4u%3A4',
            'rur':'"VLL\05444040163983\0541665432387:01f78ed5e86655931724e3a76a0753c09a3592fae5cba6ac2e7a5a5bef8a625f569eee39"',    
        }

#---------------------------------- convert response to json(dict)format ----------------------------------
def json_converter(response):
        x = response.xpath("//script[starts-with(.,'window._sharedData')]/text()").extract_first()
        json_string = x.strip().split(' = ')[1][:-1]
        data = json.loads(json_string)
        return data
        
#---------------------------------- parse taged people from list of them ----------------------------------
def pars_tagged_people(people:list)->list:
    output =[]
    #chaecking if ther is any person tagged
    if len(people)==0:
        return output
    #loop over people
    for persion in people:
        user=persion['node']["user"]
        data={
                    "username": str(user["username"]),
                    "fullname": str(user["full_name"]),
                    "is_verified": str(user["is_verified"])
                }
        output.append(data)
    return output

#---------------------------------- parse posts from list of edges ----------------------------------
def pars_edges(edges:list,output:dict,key:str):
    result = list(output[key])
    #loop over edges
    for edge in edges:
        edge=edge['node']
        #extract type
        type={
            "from":"posts",
            "type":"image"
        }
        if edge['is_video']:
            type['type']="video"
        if key=="igtvposts":
            type['from']="igtv"
        #exract owner
        owner={}
        try:
            owner=edge['owner']
        except:
            owner={}
        #extact caption
        captions = ""
        try:
            if edge['edge_media_to_caption']:
                if len(edge['edge_media_to_caption']['edges'])==1:
                    captions=edge['edge_media_to_caption']['edges'][0]['node']['text']
                else:
                    for cap in edge['edge_media_to_caption']['edges']:
                        captions += cap['node']['text']
        except:
            captions=""
        #extract location
        location=""
        try:
            #igtv does not have location
            if key !="igtvposts":
                if edge['location']:
                    location=str(edge['location'])
        except:
            location=""
        #extract instagram describe of post
        instagram_describe=""
        try:
            if edge['accessibility_caption']:
                instagram_describe=str(edge['accessibility_caption']).replace(',',";")
        except:
            instagram_describe=""
        #extract tagged people
        tagged = []
        try:
            tagged= pars_tagged_people(edge['edge_media_to_tagged_user']['edges'])
        except:
            tagged=[]
        #create data
        data = {
        "owner":owner,
        "date_of_creation": str(datetime.fromtimestamp(edge['taken_at_timestamp']).strftime("%d/%m/%Y %H:%M:%S")),
        "type": type,
        "caption":captions,
        "number_of_like": str(edge['edge_media_preview_like']['count']),
        "number_of_comment":str(edge['edge_media_to_comment']['count']),
        "location": location,
        "tagged_people":tagged,
        "instagram_describe": instagram_describe
        }
        #append data to final output of function
        result.append(data)
        send_to_kafka(data)
    #adding all posts that function extract to final output
    d={key:result}
    output.update(d)

#---------------------------------- making path with username  ----------------------------------
def make_path(name:str)->str:
    return (os.path.dirname(__file__)+"/../../../../scrapy_data/"+name+".json")

#---------------------------------- spider ----------------------------------
class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    allowed_domains = ['instagram.com']
    handle_httpstatus_all = True
    #---------------------------------- start function that loop over usernames ----------------------------------
    def start_requests(self):
        #loop over username list
        for username in user_accounts:
            url = f'https://www.instagram.com/{username}/'
            yield scrapy.Request(url,cookies=cookies, callback=self.parse)
    
    #---------------------------------- done function chek output status and if all three status ==true write output to the file ----------------------------------
    def done(self,output:dict):
        if output['userinfostatus'] and output['igtvstatus'] and output['postsstatus']:
            username=output['userinfo']['username']
            output.pop('userinfostatus')
            output.pop('igtvstatus')
            output.pop('postsstatus')
            path = make_path(username)
            file = open(path,"w")
            json.dump(output,file)
            file.close()
            print("\n    done    \n")

    #---------------------------------- if igtv posts more than 12 then this function get all igtv posts ----------------------------------
    def parse_igtvposts(self, response,output:dict):
        di = response.meta['pages_di']
        data = json.loads(response.text)

        pars_edges(data['data']['user']['edge_felix_video_timeline']['edges'],output,"igtvposts")
        #checking if there is next page
        next_page_bool = data['data']['user']['edge_felix_video_timeline']['page_info']['has_next_page']
        if next_page_bool:
            #create requets to the instagram server to get next 12 posts
            cursor = data['data']['user']['edge_felix_video_timeline']['page_info']['end_cursor']
            di['after'] = cursor
            params = {'query_hash': 'bc78b344a68ed16dd5d7f264681c4c76', 'variables': json.dumps(di)}
            url = 'https://www.instagram.com/graphql/query/?' + urlencode(params)
            yield scrapy.Request(url, callback=self.parse_igtvposts,cb_kwargs={"output":output}, meta={'pages_di': di},cookies=cookies)
        else:
            #set igtv status true and call done function
            output['igtvstatus']=True
            self.done(output)

    #---------------------------------- if posts more than 12 then this function get all posts ----------------------------------
    def parse_pots(self, response,output:dict):
        di = response.meta['pages_di']
        data = json.loads(response.text)
        
        pars_edges(data['data']['user']['edge_owner_to_timeline_media']['edges'],output,"posts")
        
        #checking if there is next page
        next_page_bool = data['data']['user']['edge_owner_to_timeline_media']['page_info']['has_next_page']
        if next_page_bool:
            #create requets to the instagram server to get next 12 posts
            cursor = data['data']['user']['edge_owner_to_timeline_media']['page_info']['end_cursor']
            di['after'] = cursor
            params = {'query_hash': '8c2a529969ee035a5063f2fc8602a0fd', 'variables': json.dumps(di)}
            url = 'https://www.instagram.com/graphql/query/?' + urlencode(params)
            yield scrapy.Request(url, callback=self.parse_pots,cb_kwargs={"output":output}, meta={'pages_di': di},cookies=cookies)
        else:
            #set post status true and call done function
            output['postsstatus']=True
            self.done(output)

    #---------------------------------- parse pages ----------------------------------
    def parse(self, response): 
        #convert response to dict       
        input = json_converter(response)
        #extract user
        user=input["entry_data"]["ProfilePage"][0]["graphql"]["user"]
        #extract biography
        biography =""
        try:
            if user['biography']:
                biography=str(user['biography'])
        except:
            biography=""
        #create final output dict
        output ={
        "userinfostatus":True,
        "igtvstatus":False,
        "postsstatus":False,
        
        "userinfo": {
            "username": str(user['username']),
            "fullname": str(user['full_name']),
            "biography": biography,
            "is_verified": str(user['is_verified']),
            "number_of_igtv_posts":str(user['edge_felix_video_timeline']['count']),
            "number_of_posts":str(user['edge_owner_to_timeline_media']['count']),
            "number_of_followers":str(user['edge_followed_by']['count'] ),
            "number_of_folowing": str(user['edge_follow']['count'])
        },
        "igtvposts":[],
        "posts": []
        }
        #checking that user has igtv post
        if len(user['edge_felix_video_timeline']['edges'])!=0:
            pars_edges(user['edge_felix_video_timeline']['edges'],output,"igtvposts")
            #checking that user igtv posts has next page
            if user['edge_felix_video_timeline']['page_info']['has_next_page']:
                #creat request for instagram server to get next 12 igtv posts
                user_id=str(user['id'])
                cursor=user['edge_felix_video_timeline']['page_info']['end_cursor']
                di = {'id': user_id, 'first': 12, 'after': cursor}
                params = {'query_hash': 'bc78b344a68ed16dd5d7f264681c4c76', 'variables': json.dumps(di)}
                url = 'https://www.instagram.com/graphql/query/?' + urlencode(params)
                #send request and send parse_igtvposts as callback func
                yield scrapy.Request(url,callback=self.parse_igtvposts,cb_kwargs={"output":output}, meta={'pages_di': di},cookies=cookies)
            else:
                #set igtv status true and call done func
                output['igtvstatus']=True
                self.done(output)
        else:
            #set igtv status true and call done func
            output['igtvstatus']=True
            self.done(output)
        #checking that user has post
        if len(user['edge_owner_to_timeline_media']['edges'])!=0:
            pars_edges(user['edge_owner_to_timeline_media']['edges'],output,"posts")
            #checking that user posts has next page
            if user['edge_owner_to_timeline_media']['page_info']['has_next_page']:
                #creat request for instagram server to get next 12 posts
                user_id=str(user['id'])
                cursor=user['edge_owner_to_timeline_media']['page_info']['end_cursor']
                di = {'id': user_id, 'first': 12, 'after': cursor}
                params = {'query_hash': '8c2a529969ee035a5063f2fc8602a0fd', 'variables': json.dumps(di)}
                url = 'https://www.instagram.com/graphql/query/?' + urlencode(params)
                #send request and send parse_pots as callback func
                yield scrapy.Request(url,callback=self.parse_pots,cb_kwargs={"output":output}, meta={'pages_di': di},cookies=cookies)
            else:
                #set posts status true and call done func
                output['postsstatus']=True
                self.done(output)
        else:
            #set posts status true and call done func
            output['postsstatus']=True
            self.done(output)
#-----------------------------------------------------end--------------------------------------------------


        
        
        
        

