import math

# Functions for obfuscating IDs before sending them to the frontend
# Based on the formula x -> (x * multiplier) % (2 ^ n), which shuffles numbers from 0 to 2^n - 1 pretty well

power = 23  # Take an odd power of two as the base
golden = 0.618
base = 1 << power
half = (power + 1) // 2

# Whip says it shuffles well when the multiplier is close to the golden ratio from the base.
# Who are we to argue with Knut? Well, the multiplier must also be odd.
multiplier = math.floor(base * golden) // 2 * 2 + 1
# This is the inverse formula over the Z(2^n) field. Mathematics!
inverse = pow(multiplier, base - 1, 2 * base)


def shuffle(x, mult):
    # If the number is greater than the base, then you must first subtract to make it smaller, and at the end return it back
    n = x // base
    x = x % base
    # Shuffle the bytes a little
    x = (((x >> half) ^ x) * mult) % base
    x = (((x >> half) ^ x) * mult) % base
    x = (x >> half) ^ x
    # Return subtracted
    x = x + base * n
    return x


def hashid(x: int) -> str:
    x = shuffle(x, multiplier)
    # We add an extra base so that the IDs are always large
    return format(x + base, 'x')


def hashid_model(x: int, row):
    return hashid(x)


def unhashid(x: str) -> int:
    # Don't forget to remove the extra base
    x = int(x, 16) - base
    x = shuffle(x, inverse)
    return x