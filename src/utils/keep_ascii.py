def keep_ascii(s):
    ascii_part = [c for c in s if ord(c) < 128]
    x = ''.join(ascii_part).strip()
    return ' '.join(x.split())
