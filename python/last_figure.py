import numpy as np
import matplotlib.pyplot as plt
import matplotlib

font = {'family' : 'normal',
        'weight' : 'bold',
        'size'   : 16}

matplotlib.rc('font', **font)
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42



fig, ax = plt.subplots(figsize=(10, 4))

# Example data
people = ('Topic Query/Empty', 'Topic Query/Full', 'Register Topic/Empty', 'Register Topic/Full')
y_pos = np.arange(len(people))
performance = [0.5941, 67.491, 55.233, 58.435]
err = [0.00594, 1.00562, 1.96, 1.0686]
error = np.random.rand(len(people))

ax.barh(y_pos, performance, xerr=err, align='center')
#ax.set_yticks(y_pos, labels=people)

ax.set_yticks(y_pos)
# ... and label them with the respective list entries
ax.set_yticklabels(people)
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xlabel('Time[us]')
#ax.set_title('How fast do you want to go today?')

plt.show()

