import random


value = random.uniform(1, 10)      #include 1 but exclude 10 and return float
print(value)

value = random.randint(1, 6)        # include both 1 and 6

colors = ['red', 'black']

results = random.choices(colors, k=10, weights=[18, 10])
print(results)

deck = list(range(1, 52))

random.shuffle(deck)

print(deck)

hand = random.sample(deck, k=5)
print(hand)