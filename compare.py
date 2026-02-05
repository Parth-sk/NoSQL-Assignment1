with open("base.txt") as f1, open("output.txt") as f2:

    a = f1.readlines()
    b = f2.readlines()

total = len(a)
diff = 0

for i in range(total):
    if i >= len(b) or a[i] != b[i]:
        diff += 1

match = total - diff
acc = (match / total) * 100

print("Total lines:", total)
print("Different lines:", diff)
print("Accuracy:", round(acc,2), "%")
