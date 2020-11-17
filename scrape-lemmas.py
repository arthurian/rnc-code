import sys
from bs4 import BeautifulSoup
import requests
import csv
import time
import pandas as pd
import os

input_path = f'input/lemmas.tsv'
output_path = f'output/rnc_scrape_lemmas.csv'
output_columns = ['lemma','docs','occurrences']

input_df = pd.read_csv(input_path, header=0, names=['lemma'], delimiter="\t", low_memory=False)
try:
    previous_output_df = pd.read_csv(output_path, usecols=output_columns, delimiter="\t")
    previous_output_df = previous_output_df.dropna()  # dropping NA (lemmas with no data) so we can retry those
except pd.errors.EmptyDataError:
    previous_output_df = pd.DataFrame([], columns=output_columns)

# get list of lemmas to query based on any previous output
input_lemmas = set(input_df['lemma'].values.tolist()) 
previous_output_lemmas = set(previous_output_df['lemma'].values.tolist())
lemmas = input_lemmas - previous_output_lemmas

RNC_URL = 'https://processing.ruscorpora.ru/search.xml?env=alpha&api=1.0&mycorp=&mysent=&mysize=&mysentsize=&dpp=&spp=&spd=&mydocsize=&mode=main&lang=ru&sort=i_grtagging&nodia=1&text=lexgramm&parent1=0&level1=0&lex1={lemma}&gramm1=&sem1=&flags1=&sem-mod1=sem&sem-mod1=sem2&parent2=0&level2=0&min2=1&max2=1&lex2=&gramm2=&sem2=&flags2=&sem-mod2=sem&sem-mod2=sem2'

def make_request(lemma, retries=3):
    try:
        r = requests.get(RNC_URL.format(lemma=lemma))
    except requests.exceptions.ConnectionError as e:
        print(e)
        print("connection error - wait 60 seconds and then retry the request")
        time.sleep(60)
        return make_request(lemma, retries=retries-1)

    if r.status_code != 200:
        print(r.status_code, lemma)
    if r.status_code == 429: 
        wait_time = int(10 + 60 * (1/retries)) if retries > 0 else 120
        print(f"too many requests -- wait {wait_time} seconds before retrying ({retries} left)")
        time.sleep(wait_time)
        if retries > 0:
            return make_request(lemma, retries=retries-1)
    return r.content


def get_data(lemma, content):
    """get # of docs with and occurrences of a word in the full RNC"""
    soup = BeautifulSoup(content, 'html.parser')
    for d in soup.findAll('div', attrs={'class':'content'}):
        for p in soup.findAll('p', attrs={'class':'found'}):
            stats = [span.text.replace(' ','') for span in d.findAll('span', attrs={'class':'stat-number'})]
            if stats != []:
                docs, occurrences = stats[2], stats[3]
                return [lemma, docs, occurrences]
    return [lemma, None, None]


with open(output_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter='\t')
    csv_writer.writerows([output_columns] + previous_output_df.values.tolist())
    csvfile.flush()
    for i, lemma in enumerate(lemmas):
        time.sleep(1)
        content = make_request(lemma)
        current_row = get_data(lemma, content)
        try:
            csv_writer.writerow(current_row)
            csvfile.flush()
            print(current_row)
            if (i % 1000) == 0:
                print(f"{i}/{len(lemmas)} lemmas parsed.")
        except Exception as e:
            print('Error running get_data on', lemma)
            print(e)
