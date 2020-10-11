from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
from cleanco import cleanco
import pandas as pd
import random
from utils import create_connection, persistant_cache
import mysql.connector
import json


try:
    with open(".secret.json") as f:
        pws = json.load(f)
        mysql_pw = pws["mysql"]
        paw_pw = pws["pythonanywhere"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    pass

general_terms = [
    'construction',
    'constructor',
    'constructors',
    'contracting',
    'contractor',
    'contractors',
    'contract',
    'contracts',
    'general',
    'associates',
    'service',
    'services',
    'building',
    'buildings',
    'builders',
    'build',
    'design',
    'prime',
    'facility',
    'group',
    'groupe',
    'international',
    'project',
    'projects',
    'management',
    'corporate',
    'inc',
    'incorporated',
    'corp',
    'corporation',
    'corporations',
    'ltd',
    'limited',
    'company',
    'companies',
    'business',
    'businesses',
    'enterprise',
    'enterprises',
    'co',
    'partnership',
    'development',
    'developments',
    'industries',
    'associate',
    'associates',
    'tech',
    'technology',
    'technologies',
    'innovation',
    'innovations',
    'energy',
    'managers',
    'management',
    'commercial',
    'trade',
    'maintenance',
    'project',
    'leaders',
    'division',
    'systems',
    'restoration',
    'resources',
    'property',
    'properties',
    'joint',
    'venture',
    'campus',
    'facilities',
    'infrastructure',
    'infrastructures',
    'department',
    'dept',
    'healthcare',
    'city',
    'county',
    'town',
    'region',
    'municipality',
    'village',
    'township',
    'province',
    'attn',
    'attention',
    'represented',
    'investment',
    'investments',
    'holdings',
    'area',
]
dvision_terms = [
    'mechanical',
    'sheet',
    'metal',
    'air',
    'conditioning',
    'ac',
    'plumbing',
    'heating',
    'hvac',
    'cooling',
    'climate',
    'refrigeration',
    'insulation',
    'controls',
    'fire protection',
    'controls',
    'electrical',
    'electric',
    'power',
    'utilities',
    'utility',
    'paving',
    'pave',
    'pavement',
    'excavating',
    'excavation',
    'landscaping',
    'landscape',
    'landscapes',
    'lanscapers',
    'equipment',
    'equipments',
    'rental',
    'rentals',
    'product',
    'products',
    'industrial',
    'residential',
    'residences',
    'residence',
    'housing',
    'house',
    'houses',
    'masonry',
    'millwork',
    'woodwork',
    'drywall',
    'structural',
    'engineering',
    'engineers',
    'eng',
    'peng',
    'engineer',
    'architecture',
    'architect',
    'architects',
    'design',
    'consulting',
    'consultants',
    'welding',
    'steel',
    'piling',
    'fabricator',
    'fabricators',
    'fabrication',
    'elevator',
    'foundation',
    'foundations',
    'forms',
    'forming',
    'concrete',
    'roofing',
    'painting',
    'interior',
    'interiors',
    'glazing',
    'cladding',
    'hardware',
    'caulking',
]

geographic_locations = [
    'canada',
    'canadian',
    'ontario',
    'ontarian'
]

def grey_color_func(word, font_size, position, orientation, random_state=None, **kwargs):
    return "hsl(268, 17%%, %d%%)" % random.randint(0, 45)

@persistant_cache('static/wc_cache.json')
def generate_wordcloud(term_field):
    field = term_field.split('_')[-1]
    term = '_'.join(term_field.split('_')[:-1])
    query = """
        SELECT {}
        FROM web_certificates
        WHERE cert_id in (
            SELECT cert_id 
            FROM cert_search 
            WHERE text MATCH %s
        )
    """
    with create_connection() as conn:
        df = pd.read_sql(query.format(field), conn, params=[term])
    df['contractor_clean'] = df[field].apply(lambda x: cleanco(x).clean_name())
    relevant_words = [word.lower().lstrip().rstrip().replace('.','') for word in df['contractor_clean']]
    relevant_text = " ".join(relevant_words)
    stopwords = set(STOPWORDS)
    stopwords.update(general_terms + dvision_terms + term.split(' '))
    if field != 'owner':
        stopwords.update(geographic_locations)
    try:
        wordcloud = WordCloud(stopwords=stopwords, background_color=None, mode='RGBA', width=1000, height=400, color_func=lambda *args, **kwargs: "black").generate(relevant_text.upper())
        if len(wordcloud.words_):
            wordcloud.recolor(color_func=grey_color_func, random_state=3)
            wordcloud.to_file(f"static/wordcloud_{term.replace(' ', '_')}_{field}.png")
        return len(df), len(wordcloud.words_)/len(df)
    except ValueError:
        pass  # search term did not generate enough words
        return len(df), 0
    
