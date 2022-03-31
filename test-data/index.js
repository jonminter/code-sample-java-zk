const faker = require('faker');
const uuid = require('uuid');
const fs = require('fs');
const path = require('path');

// Scenario: Mega-Tech Company
// Org structure:
// CEO -> MVP -> VP -> Sr Director -> Director -> Manager -> Cogs in the Machinery

// Number of employees (minus CEO/dept heads)
const numEmployees = 100000;
// Number of employees to save to each data file
const maxBatchSize = 5000;

// Distribution of employees in each department
// Used to assign department to each generated employee
const departmentPercentages = {
  "Corporate": 1,
  "Data Engineering": 15,
  "HR": 5,
  "Platform Engineering": 15,
  "Product": 10,
  "Product Engineering": 50,
  "Recruiting": 4,
};
const departments = Object.getOwnPropertyNames(departmentPercentages);

const countsPer = {
  department: {}
};
const departmentEmployees = {
};

const employees = [];
const employeeNameMap = {};

function createEmployee() {
  const id = uuid.v4();
  const name = faker.name.findName();
  employeeNameMap[id] = name;
  return {
    id,
    name,
    manager_id: null,
  };
}

function getRandomIntInclusive(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1) + min);
}

function range(size) {
  return [...Array(size).keys()];
}

// Create employees and assign them to a department
const weightedDepartments = departments.reduce(
  (accum, dept) => accum.concat(range(departmentPercentages[dept]).map(i => dept)),
  []);

for (let i = 0; i < numEmployees; i++) {
  const employee = createEmployee(); 
  const department = faker.random.arrayElement(weightedDepartments);
  countsPer.department[department] = countsPer.department[department] || 0;
  countsPer.department[department]++;

  departmentEmployees[department] = departmentEmployees[department] || [];
  departmentEmployees[department].push(employee);
  employees.push(employee);
}

// Assigns employees managerial roles and assigns them managers from 
// the level above them (i.e. managers are direct reports of directors)
function getManagerList(employees, numOfManagers, managersOfManagers) {
  const managerList = [];
  for (let i = 0; i < numOfManagers; i++) {
    faker.helpers.shuffle(employees);
    const employee = employees.pop();
    if (managersOfManagers) {
      employee.manager_id = faker.random.arrayElement(managersOfManagers).id;
    }
    managerList.push(employee);
  }
  return managerList;
}

const outDir = path.join(__dirname, 'out');
if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir);
}

// 20% are managers
// 4 levels 2,3,5,10
//          VP,SD,D,M
//

const departmentList = [];

const ceo = createEmployee();
const mvps = [];

// Maintain a list that has all the employees in the proper sorted order
// 1. Ordered by department first
// 2. First row of each department is the department head (i.e. the MVP)
// 3. The remaining employees in the department are ordered by their name
let allEmployeesSortedCorrectly = [{...ceo, manager_name: "", department: "Corporate"}];


// For each department builds up managerial graph (who manages who) and
// adds the employees in the right order to the sorted list
// and builds the final department list with ID and head of dept listed
for(const department of departments) {

  const numEmployeesInDept = departmentEmployees[department].length;
  console.log(`Number of employees in department '${department}' is ${numEmployeesInDept}`);

  const headOfDept = createEmployee();
  console.log(`Head of department is ${headOfDept.name}`);
  headOfDept.manager_id = ceo.id;
  mvps.push(headOfDept);

  const employeesCopy = [...departmentEmployees[department]];
  const vps = getManagerList(employeesCopy, Math.floor(0.02 * numEmployeesInDept), [headOfDept]);
  console.log(`${vps.length} VPs`);
  const srDirectors = getManagerList(employeesCopy, Math.floor(0.03 * numEmployeesInDept), vps);
  console.log(`${srDirectors.length} Senior Directors`);
  const directors = getManagerList(employeesCopy, Math.floor(0.05 * numEmployeesInDept), srDirectors);
  console.log(`${directors.length} Directors`);
  const managers = getManagerList(employeesCopy, Math.floor(0.1 * numEmployeesInDept), directors);
  console.log(`${managers.length} Managers`);
  console.log(`${employeesCopy.length} employees left over`);

  getManagerList(employeesCopy, employeesCopy.length, managers);

  if (department === "Corporate") {
    departmentEmployees[department].push(headOfDept);
  } else {
    allEmployeesSortedCorrectly.push({ ...headOfDept, manager_name: ceo.name, department });
  }
  allEmployeesSortedCorrectly = allEmployeesSortedCorrectly.concat(
    departmentEmployees[department]
      .map(e => ({...e, department, manager_name: employeeNameMap[e.manager_id]}))
      .sort((a,b) => a.name.localeCompare(b.name))); 

  if (department != "Corporate") {
    departmentList.push({
      id: uuid.v4(),
      name: department,
      department_head_id: headOfDept.id,
    });
  }
}

employees.push(ceo);
mvps.map(mvp => employees.push(mvp));

// Shuffle one last time after adding CEO/MVPs
faker.helpers.shuffle(employees);

// Create CSV to use to test output of distributed merge sort against
const employeeCsv = 'id,name,department,manager\n' + 
  allEmployeesSortedCorrectly
    .map(e => `${e.id},${e.name},${e.department},${e.manager_name}`)
    .join('\n') + '\n';
fs.writeFileSync(`${outDir}/employees-sorted-correctly.csv`, employeeCsv);
delete employeeCsv;

// Create employee data files. Split into batches 
const outputFilePrefix = `employees`;
let numLeft = employees.length;
console.log(`Total num of employees: ${numLeft}`);
let batchStart = 0;
let batchNumber = 1;
while (numLeft > 0) {
  const batchSize = Math.min(numLeft, maxBatchSize);

  const batch = employees.slice(batchStart, batchStart + batchSize);

  const outputFile = `${outDir}/${outputFilePrefix}.${batchNumber}.json`;
  console.log(`Writing employee records ${batchStart} to ${batchStart + batchSize} into ${outputFile}`);
  fs.writeFileSync(outputFile, JSON.stringify(batch, null, 2));

  batchStart += batchSize;
  numLeft -= batchSize;
  batchNumber++;
}

fs.writeFileSync(`${outDir}/departments.json`, JSON.stringify(departmentList, null, 2));

  
