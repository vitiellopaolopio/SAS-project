*/ SET PATH AND ASSIGN LIBRARY */  

%let datapath=/home/u64342387/Project; 
libname src "/home/u64074505/PROJECT";  

/* MACRO TO LOAD DATASET TO LIBRARY */   

%macro load(ds);   
data &ds;   
set src.&ds;   
run;   

%mend;  
%load(employee_organization);   
%load(employee_addresses);   
%load(employee_donations);   
%load(employees);  

/* MACRO TO SORT DATASET */  

%macro sort(ds, byvar);   
proc sort data=&ds;  
by &byvar;   
run;   

%mend;  
%sort(Employee_organization, Employee_ID);   
%sort(Employee_addresses, Employee_ID);   
%sort(Employee_donations, Employee_ID);   
%sort(Employees, Employee_ID);  

/*  MERGE ALL DATASETS INTO FINAL EMPLOYEE DATASET */  
/*  merge them one at a time to keep all the variables */ 
/* merging employees and employee_donations */ 

data Employee; 
    merge Employees(in=a) 
          Employee_donations(in=b); 
    by Employee_ID; 
run; 

/* adding employee_addresses */ 
data Employee; 
   merge Employee 
          Employee_addresses; 
    by Employee_ID; 
run; 

/* adding employee_organization */ 
data Employee; 
    merge Employee 
          Employee_organization; 
    by Employee_ID; 
run; 

/*   APPLY FILTERS REQUIRED BY THE PROJECT*/  
data Employee;  
    set Employee;  
     if Department not in ("Accounts","Accounts Management","IS")  
       and Qtr1 > 0
       and Qtr4 > 0;  
run;  

/*  CREATE NEW VARIABLES REQUIRED BY REPORT 1*/  
proc format;  
    value $payfmt  
        "Payroll Deduction" = "PD"  
        "Credit Card"       = "CC"  
        other               = "Other";  
run;  
data Employee_report1;  
    set Employee;  
    SUBJID = cats(upcase(Country), "-", Employee_ID);
    EMPLOYEE = upcase(substr(Name,1,2));  
    PAYMENT = put(Paid_By, $payfmt.);  
    COUNT_DON = sum(Qtr1, Qtr2, Qtr3, Qtr4);  
    if Qtr1>0 and Qtr2>0 and Qtr3>0 and Qtr4>0 then ALLQ=1;  
    else ALLQ=0;  
    format COUNT_DON dollar8.2;  
run;  

/* MANAGER TABLE*/  
/* 6A. Manager salary aggregation using FIRST. and LAST. (subordinates -> manager)  
       We compute AvgSalary of subordinates per Manager_ID. */ 
/* Sort by Manager_ID so subordinates are grouped under the manager key */  
proc sort data=Employee out=EmpByMgr;   
by Manager_ID Employee_ID; /* secondary sort to keep subordinate ordering */   

run;  

data ManagerSalary;   
set EmpByMgr;   
by Manager_ID;  
retain SumSalary CountSalary;  
  
/* initialize at start of group */  
if first.Manager_ID then do;  
    SumSalary = 0;  
    CountSalary = 0;  
end;  
  
/* only count actual subordinates: if Manager_ID is missing, skip;  
   also avoid counting manager's own row if present among subordinates by checking Employee_ID ne Manager_ID */  

if not missing(Manager_ID) and Employee_ID ne Manager_ID and not missing(Salary) then do;  
    SumSalary + Salary;  
    CountSalary + 1;  
end;  
  
/* at end of manager group, compute avg and output one row per manager if there are subordinates */  
if last.Manager_ID then do;  
    if CountSalary > 0 then AvgSalary = SumSalary / CountSalary;  
    else AvgSalary = .;  
    output;  
end;  

keep Manager_ID AvgSalary;  
run;  

 /* 6B. Build ManagerInfo: the rows in Employee that correspond to managers (i.e., Employee_ID appears as Manager_ID)  
Approach:  
 - extract distinct Manager_IDs (from Employee table)  
 - match to Employee data to retrieve demographic fields for that manager (city, department, qrt1-4)  
*/ 
proc sort data=Employee(keep=Manager_ID) out=DistinctManagers nodupkey;   
by Manager_ID;  
run;  

/* remove missing Manager_ID from the distinct list */   

data DistinctManagers;   
set DistinctManagers;   
if not missing(Manager_ID);   
run;  

/* Ensure Employee has a record for the manager (by Employee_ID) */   
proc sort data=Employee out=EmpByID;   
by Employee_ID;   
run;  

proc sort data=DistinctManagers;   
by Manager_ID;   
run;  

/* Merge to extract manager rows from Employee */  
data ManagerInfo;   
merge EmpByID   

(in=e   
rename=(Employee_ID=MgrKey   
City=MgrCity   
Department=MgrDept   
Qtr1=Mgr_q1  
Qtr2=Mgr_q2  
Qtr3=Mgr_q3   
Qtr4=Mgr_q4))   
DistinctManagers   
(in=d 
rename=(Manager_ID=MgrKey));   
by MgrKey;  

/* Keep only rows where Employee_ID is one of the Manager_ID values */  
if e and d;
  
/* Reconstruct standard variable names */  
Manager_ID = MgrKey;  
City       = MgrCity;  
Department = MgrDept;  
Qtr1       = Mgr_q1;  
Qtr2       = Mgr_q2;  
Qtr3       = Mgr_q3;  
Qtr4       = Mgr_q4;  
keep Manager_ID City Department Qtr1 Qtr2 Qtr3 Qtr4;  
run;  

/* 6C. Merge ManagerInfo + ManagerSalary -> ManagerFull */ 
proc sort data=ManagerInfo;   
by Manager_ID;   
run;   

proc sort data=ManagerSalary;   
by Manager_ID;   
run;  

data ManagerFull;  
merge ManagerInfo(in=a) ManagerSalary(in=b);   
by Manager_ID;  

/* keep manager rows even if AvgSalary is missing (manager with no subordinates) */   
if a;   
run;  

/* 6D. Convert wide Qtr1-4 to long (one row per Period) */ 
data ManagerTableFinal;  
set ManagerFull;  
length Period $4;   
Period = "Qtr1";   
Donation = Qtr1;   
output; Period = "Qtr2";   
Donation = Qtr2;   
output;   
Period = "Qtr3";   
Donation = Qtr3;   
output;   
Period = "Qtr4";   
Donation = Qtr4;   
output;   
keep Manager_ID City Department AvgSalary Period Donation;   
run;  

proc sort data=ManagerTableFinal;   
by Manager_ID Period;   
run;  

proc print data=ManagerTableFinal noobs label; 
     var Manager_ID City Department AvgSalary Period Donation; 
     label Manager_ID = "Manager ID" 
           City = "City" 
           Department = "Department" 
           AvgSalary = "AvgSalary" 
           Period ="Period" 
           Donation ="Donation"; 
     title "Manager Donations and Average Subordinate Salary"; 
run; 

/*  ARRAY TO COMPUTE 10 % INCREASES IN DONATIONS*/  
data Employee_donations_10;   
set Employee;   
array d[4] Qtr1 Qtr2 Qtr3 Qtr4;   
array inc[4] Qtr1_10 Qtr2_10 Qtr3_10 Qtr4_10;  
do i = 1 to 4;  
    inc[i] = d[i] * 1.10;  
end;  
drop i;  
run;  

/*  YOUNGEST AND OLDEST DONOR DY DEPARMENT */  
/* Creation of the variable Age */ 

data Employee; 
    set Employee; 
    Age=floor((today()-Birth_Date)/365.25); 
    COUNT_DON=sum(Qtr1, Qtr2, Qtr3, Qtr4);    
run; 

/* keep only donors */ 
data Donors; 
    set Employee; 
    if COUNT_DON > 0; 
run; 

/* sort donors by Department and Age */ 
proc sort data=Donors out=DonorsSorted; 
    by Department Age; 
run; 

data YoungOld_Donors;  
    set DonorsSorted;  
    by Department Age;  

/* First age in department: youngest */ 

if first.Department then do; 
  AgeType = "Youngest Donor"; 
   output; 
end; 
 
/* Last age in department: oldest */ 
if last.Department then do; 
   AgeType = "Oldest Donor"; 
   output; 
end; 
keep Department Employee_ID Name City Age AgeType; 
run; 

/* see the results */ 
proc sort data=YoungOld_Donors; 
     by Department AgeType; 
run; 
 
proc print data=YoungOld_Donors noobs label; 
     var Department AgeType Age Employee_ID Name City; 
     label AgeType = "Donor Type" 
           Employee_ID = "Employee ID"; 
     title "Youngest and Oldest Donors by Department"; 
run; 

/*   SUMMARY TABLE */  
proc means data=Employee n mean median min max maxdec=2;  
    class Gender Marital_Status;  
    var   COUNT_DON;  
    label Gender = "Gender" 
         Marital_Status = "Marital Status" 
          COUNT_DON = "Total Donations"; 
    title "Summary of Total Donations by Gender and Marital Status"; 
run;  

/* CREATE PDF REPORT*/ ods pdf file="/home/u64074505/PROJECT/Employee_Report.pdf" 
style=journal 
bookmarkgen=yes 
bookmarklist=hide; 
/* ============================ REPORT 1 — EMPLOYEE SUMMARY ============================ */ 
title "Employee Donation Summary Report"; 
proc print data=Employee_report1 label noobs; 
var SUBJID EMPLOYEE PAYMENT COUNT_DON ALLQ; 
label SUBJID = "Employee Identifier" 
EMPLOYEE = "Initials" 
PAYMENT = "Payment Method"
 COUNT_DON = "Total Donations" 
 ALLQ = "All Quarters Donated";
 run; 
 /* ============================ REPORT 2 — MANAGER SUMMARY ============================ */ 
title "Manager Donation Summary"; 
proc print data=ManagerTableFinal label;
 var Manager_ID City Department AvgSalary Period Donation; 
 label Manager_ID = "Manager ID" City = "City" Department = "Department" AvgSalary = "Average Salary of Subordinates" Period = "Donation Period" Donation = "Donation Amount";
 run;
 /* ============================ REPORT 3 — YOUNGEST & OLDEST ============================ */ 
title "Youngest and Oldest Donor in Each Department"; 
proc print data=YoungOld_Donors label; 
var Department Employee_ID Name City Age AgeType; 
label Department = "Department" Employee_ID = "Employee ID" Name = "Name" City = "City" Age = "Age" AgeType = "Donor Type"; 
run;
 /* ============================ REPORT 4 — SUMMARY STATS ============================ */ 
title "Donation Summary by Gender and Marital Status";
 proc means data=Employee n mean median min max maxdec=2; 
 class Gender Marital_Status;
 var COUNT_DON; 
 label Gender = "Gender" Marital_Status = "Marital Status" COUNT_DON = "Total Donations"; 
 run; 
 ods pdf close;
 title;