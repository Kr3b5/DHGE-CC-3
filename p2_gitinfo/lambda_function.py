############################################################################
## Lambda Funktion - Github Info
############################################################################
##
############################################################################
## Author: Kevin Lempert
## Copyright: Copyright 2021, CC3
## Version: 1.0.5
## Status: prod
############################################################################

import requests
import json

############################################################################
# GLOBAL VARS
############################################################################

json_user = {}
json_user_github = {}
json_user_gitlab = {}
json_repo = {}
jsonarray_repo_github = []
jsonarray_repo_gitlab = []

user_keys = ["id", "name", "alias", "url"]
user_github_keys = ["id", "name", "login", "html_url"]
user_gitlab_keys = ["id", "name", "username", "web_url"]

repo_keys = ["id", "name", "private", "url", "forks", "language"]
repo_github_keys = ["id", "name", "private", "html_url", "forks", "language"]
repo_gitlab_keys = ["id", "name", "visibility", "web_url", "forks_count", ""]

github_api_url = 'https://api.github.com/users/'
gitlab_api_url = 'https://gitlab.com/api/v4/users/'

############################################################################
# CODE
############################################################################


def lambda_handler(event, context):

    #get request parameters
    gh_id = event["queryStringParameters"]["gh_id"]
    gl_id = event["queryStringParameters"]["gl_id"]
    gl_token = event["queryStringParameters"]["gl_token"]

    # User Request 
    r_user_github = requests.get(github_api_url + gh_id).json()
    r_user_gitlab = requests.get(gitlab_api_url + gl_id).json()

    #read in Github + GitLab user values
    for i in range(len(user_keys)):
        json_user_github.update(
            {user_keys[i]: r_user_github[user_github_keys[i]]})
        json_user_gitlab.update(
            {user_keys[i]: r_user_gitlab[user_gitlab_keys[i]]})

    # Repo Request
    r_repo_github = requests.get(github_api_url + gh_id + '/repos').json()
    headers = {"Authorization": "Bearer " + gl_token}
    r_repo_gitlab = requests.get(
        gitlab_api_url + gl_id + '/projects?private=true', headers=headers).json()

    #read in Github repo values  
    for projects in r_repo_github:
        json_repo = {}
        for i in range(len(repo_github_keys)):
            if(repo_github_keys[i] != ""):
                json_repo.update({repo_keys[i]: projects[repo_github_keys[i]]})
        jsonarray_repo_github.append(json_repo)

    #read in GitLab repo values
    for projects in r_repo_gitlab:
        json_repo = {}
        for i in range(len(repo_gitlab_keys)):
            if(repo_gitlab_keys[i] == "visibility"):
                if(projects[repo_gitlab_keys[i]] == "private"):
                    json_repo.update({repo_keys[i]: "true"})
                else:
                    json_repo.update({repo_keys[i]: "false"})
            elif(repo_gitlab_keys[i] != ""):
                json_repo.update({repo_keys[i]: projects[repo_gitlab_keys[i]]})
        jsonarray_repo_gitlab.append(json_repo)

    # add repo json array to user json array
    json_user_github.update({"repos": jsonarray_repo_github})
    json_user_gitlab.update({"repos": jsonarray_repo_gitlab})

    #return complete json
    return {
        'statusCode': 200,
        'body': json.dumps({"github": json_user_github, "gitlab": json_user_gitlab})
    }
