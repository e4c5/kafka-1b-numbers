total = 0
with open('counts.txt', 'r') as file:
    for line in file:
        _, num = line.split(':')
        total += int(num)

print(f'Total: {total}')