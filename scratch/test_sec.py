from dpif.code.parser import analyze_source
ca = analyze_source('aws_key = "AKIA1234567890ABCDEF"', 'sec.py')
print('Secrets detected:', ca.secrets)
