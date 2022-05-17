import re

# Validation for display name
regexp = re.compile('[^a-zA-Z0-9\s.]')
def displayNameValidation(displayname):
    if regexp.search(displayname):
        return None
    else:
        return displayname

mystring = "Rahul12@ Wangawar"
print(displayNameValidation(mystring))

# removing special characters
def name_validation(name):
    return re.sub('[^A-Za-z0-9]+', ' ', name.lower())


print(name_validation(mystring))

# gender formatting
"""
dict = {"M" : 'Male', "F" : 'Female', "O": 'Other', "T" : 'Transgender'}

df2=df.replace({"gender": dict})
print(df2)
"""
dict = {"M" : 'Male', "F" : 'Female', "O": 'Other', "T" : 'Transgender'}
def gender_foramtting(gender):
    gen = str(gender[0]).upper()
    if gen in dict.keys():
        print(dict[gen])
    else:
        return "Other"
# gender_foramtting('dsfa')


# birthdate validation and formatting
from datetime import datetime
def birthdate(date):
    lst=[':','/','\\','-']
    format = "%d-%m-%Y"
    try:
        for i in lst:
            if i in date:
                date =date.replace(i,'-')
                return date
        else:
                date = date[:2]+'-'+date[2:4]+'-'+date[4:]
        if bool(datetime.strptime(date, format))==True:
            return date
    except:
        return None

print(birthdate("10-10-2002"))





# email




# contact number




# driving licence




# pancard





# passport



















# part5= part5.rdd.map(lambda line: Row(DisplayName=displayNameValidation(line[0]), AboutMe=line[1], BirthDate=line[2],PhoneNumber=phoneNumValidation(line[3],line[5]), Location=line[4], Country=line[5], State=line[6],City=line[7])).cache()