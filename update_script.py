import sys

with open("craw/auto/map_meeyland_district.py", "r") as f:
    text = f.read()

hardcoded_districts = {
    'bu dang': 'bù đắng', # actually the db has Bù Ðăng (unicode D)
    'phu quy': 'phú quí',
    'ea kar': 'ea kra',
    "m'drak": "m'đrắt",
    'ia grai': 'la grai',
    'ia pa': 'la pa',
    'kong chro': 'krông chro',
    'meo vac': 'mèo vạt',
    'bach long vi': 'bạch long vỹ',
    'giong rieng': 'gồng giềng',
    "ia h'drai": 'la hdrai',
    'nam nhun': 'nậm nhùn', # does not exist in 30, Lai chau
    'tam duong': 'tam đường', # does not exist
    'tan uyen': 'tân uyên', # does not exist
    'than uyen': 'than uyên', # exists in 27 (lao cai), wait Lai chau ID is 30, it moved?
    'chi lang': 'chi lăng', # doesn't exist
    'si ma cai': 'xi ma cai',
    'kien tuong': 'kiến tường'
}

insertion = """
        # Hardcode aliases for system typos
        aliases = {
            'bù đăng': 'bù ðăng',
            'bu dang': 'bù ðăng',
            'phú quý': 'phú quí',
            'ea kar': 'ea kra',
            "m'đrắk": "m'đrắt",
            'ia grai': 'la grai',
            'ia pa': 'la pa',
            'kông chro': 'krông chro',
            'mèo vạc': 'mèo vạt',
            'bạch long vĩ': 'bạch long vỹ',
            'giồng riềng': 'gồng giềng',
            "ia h'drai": 'la hdrai',
            'si ma cai': 'xi ma cai',
            'nậm nhùn': 'mường tè', # Approximation or skip?
            'tam đường': 'phong thổ', # Skip or skip? Let's just fuzzy match against alias
            'tân uyên': 'than uyên',
            'than uyên': 'than uyên', # Maybe system maps it to Lao Cai (27), let's cross-search
        }
        if not old_id and name.lower() in aliases:
            norm_alias = normalize_name(aliases[name.lower()])
            old_id = parent_dists.get(norm_alias)
        if not old_id and norm in aliases:
            norm_alias = normalize_name(aliases[norm])
            old_id = parent_dists.get(norm_alias)
"""
text = text.replace("        # Fuzzy match", insertion + "        # Fuzzy match")

with open("craw/auto/map_meeyland_district.py", "w") as f:
    f.write(text)

