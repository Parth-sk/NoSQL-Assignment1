-- 1. Clean up old tables (Order matters due to Foreign Keys)
DROP TABLE IF EXISTS Grade CASCADE;
DROP TABLE IF EXISTS Student CASCADE;
DROP TABLE IF EXISTS Course CASCADE;

-- 2. Create Student Table
CREATE TABLE Student (
    student_id VARCHAR(50) PRIMARY KEY, -- Format: IMT_2023_001
    name VARCHAR(100),
    age INT,
    email VARCHAR(100)
);

-- 3. Create Course Table (Reference for Departments)
CREATE TABLE Course (
    course_id VARCHAR(20) PRIMARY KEY,
    course_name VARCHAR(100),
    department VARCHAR(50)
);

-- 4. Create Grade Table
CREATE TABLE Grade (
    student_id VARCHAR(50),
    course_id VARCHAR(20),
    score INT,
    PRIMARY KEY (student_id, course_id),
    FOREIGN KEY (student_id) REFERENCES Student(student_id)
);

-- 5. Populate Course Data (Static Data)
INSERT INTO Course (course_id, course_name, department) VALUES
('CS101', 'Intro to NoSQL', 'CS'),
('CS102', 'Operating Systems', 'CS'),
('MA101', 'Calculus I', 'Math'),
('MA102', 'Linear Algebra', 'Math'),
('PH101', 'Physics I', 'Physics');