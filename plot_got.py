import sys, re
import matplotlib.pyplot as plt

if len(sys.argv) !=2:
	print "Usage: python plot_got.py input_file"
	exit(1)
	
input_file = sys.argv[1]

track = ["arya", "balon", "bran", "brienne", "bronn", "brynden", "cersei", "daario", "daenerys", "davos", "eddard", "euron", "gilly", "gregor", "hodor", "jaime", "jon", "jorah", "lancel", "leaf", "loras", "lyannas", "lyannam", "mace", "margaery", "meera", "melisandre", "myrcella", "olenna", "olly", "osha", "petyr", "podrick", "rickon", "roose", "ramsay", "samwell", "sandor", "sansa", "sparrow", "theon", "tommen", "tormund", "tyrion", "varys", "walda", "walder", "yara"]

characters = {}

with open(input_file) as data:
	for line in data:
		parts = line.split()
		count = int(parts[1])
		key = re.split('(\d+)', parts[0])
		if key[0] not in track: continue
		if key[0] not in characters:
			characters[key[0]] = []
		characters[key[0]].append((int(key[1]), count))
		
		
for key in characters:
	if key not in ["sansa", "arya", "bran"]: continue
	characters[key].sort(key=lambda x: x[0])
	times = []
	counts = []
	for t in characters[key]:
		times.append(t[0])
		counts.append(t[1])
	plt.plot(times, counts, label=key)
plt.xlim(times[0], times[-1])	
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()