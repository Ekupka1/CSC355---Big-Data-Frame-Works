# Assignment 1 Basic Python Code- Ethan Kupka
import random
import time
import os

#Problem 1
def randLst(size):
    newLst = []
    random.seed(time.time())
    for element in range(size):
        newLst.append(random.randint(0, size+1))
    return newLst

print("Here is a random list of 4:", randLst(4))

#Problem 2
def sumLst(List):
    amount = 0
    for value in List:
        amount = amount + value
    return amount

lst = [1,2,3,4,5] #15
print("The sum is", sumLst(lst))

#Problem 3
def wordAmount(sent):
    count = 1
    for word in range(0, len(sent)):
        if sent[word] == " ":
            count+=1
    return count

newString = "Hi it is Ethan"
print("There are", wordAmount(newString),"words")

#Problem 4
file = open("words.txt", "r")
text = file.read()
file.close()
# lines = file.readLines()

def count(lst):
    lst2 = lst.split()
    wordCount = {}
    for word in lst2:
        if word in wordCount:
            wordCount[word] += 1
        else:
            wordCount[word] = 1
    return wordCount

# lst = "alice alice alice kate kate"
print(count(text)) #change to text

#Problem 5
def angrm(sent):
    angrmList = []
    for elm1 in sent:
        for elm2 in sent:
            if elm1 != elm2 and sorted(elm1) == sorted(elm2):
                angrmList.append(elm1)
    return angrmList

angrmStr = ["hi", "bye", "ih", "jk"]
print("Here are the anagrams:", angrm(angrmStr))
