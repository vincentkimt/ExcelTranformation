import csv
import random

first_names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Hannah", "Ian", "Julia"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson"]

def generate_student():
    name = f"{random.choice(first_names)} {random.choice(last_names)}"
    age = random.randint(13, 18)
    grade = random.choice(["8", "9", "10", "11", "12"])
    math = random.randint(50, 100)
    science = random.randint(50, 100)
    english = random.randint(50, 100)
    return [name, age, grade, math, science, english]

with open("large_students_data.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Name", "Age", "Grade", "Math", "Science", "English"])
    for _ in range(1_000_000):  # Change this number for more or fewer rows
        writer.writerow(generate_student())

print("CSV file with 1 million rows created successfully.")
