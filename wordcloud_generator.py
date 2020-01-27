from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
from cleanco import cleanco
import pandas as pd
import random
from utils import create_connection


general_terms = [
    'construction',
    'constructor',
    'constructors',
    'contracting',
    'contractor',
    'contractors',
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
    'canada',
    'canadian',
    'ontario',
    'ontarian',
    'project',
    'projects',
    'management',
    'corporate',
    'inc',
    'incorporated',
    'corp',
    'corporation',
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
    'project',
    'leaders',
    'division',
    'systems',
    'restoration',
    'property',
    'properties',
    'joint',
    'venture',
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
    'paving',
    'pave',
    'pavement',
    'excavating',
    'landscaping',
    'equipment',
    'equipments',
    'product',
    'products',
    'industrial',
    'masonry',
    'millwork',
    'woodwork',
    'drywall',
    'structural',
    'engineering',
    'engineers',
    'welding',
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
    'hardware',
    'caulking',
]

def grey_color_func(word, font_size, position, orientation, random_state=None, **kwargs):
    return "hsl(268, 17%%, %d%%)" % random.randint(0, 45)

def generate_wordcloud(term):
    query = """
        SELECT contractor
        FROM web_certificates
        WHERE cert_id in (
            SELECT cert_id 
            FROM cert_search 
            WHERE text MATCH ?
        )
    """
    with create_connection() as conn:
        df = pd.read_sql(query, conn, params=[term])
    df['contractor_clean'] = df.contractor.apply(lambda x: cleanco(x).clean_name())
    relevant_words = [word.lower().lstrip().rstrip().replace('.','') for word in df['contractor_clean']]
    relevant_text = " ".join(relevant_words)
    for term_word in term.split(' '):
        relevant_text = relevant_text.replace(term_word, '')
    stopwords = set(STOPWORDS)
    stopwords.update(general_terms + dvision_terms)
    wordcloud = WordCloud(stopwords=stopwords, background_color=None, mode='RGBA', width=1000, height=400, color_func=lambda *args, **kwargs: "black").generate(relevant_text)
    # wordcloud.recolor(color_func=grey_color_func, random_state=3)  # Not used for now
    wordcloud.to_file(f"static/wordcloud_{term.replace(' ', '_')}.png")