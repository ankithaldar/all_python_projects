/** @OnlyCurrentDoc */

function Year() {

  var mons = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  var days = [31   , 28   , 31   , 30   , 31   , 30   , 31   , 31   , 30   , 31   , 30   , 31   ];

  // Specify years
  var years = "2023";

  // Leap year check
  if (((years % 4 == 0) && (years % 100 != 0)) || (years % 400 == 0)) {
    if (days[1] == 28) {
      days[1] = 29;
    }
  }

  salary_bank = "HDFC"

  // create bank statements
  var banks = ['HDFC', 'SBI']

  // assert(banks.includes(salary_bank), "Salary Bank not included in bank list.")


  for (var i = 0; i < banks.length; i++) {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ss.setActiveSheet(ss.getSheetByName('BankStatement'));
    var itt = ss.getSheetByName('BankStatement - ' + banks[i]);
    if (!itt) {
      ss.duplicateActiveSheet();
      ss.getActiveSheet().setName('BankStatement - ' + banks[i]);
      ss.moveActiveSheet(ss.getNumSheets());
    }
  }
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ss = ss.setActiveSheet(ss.getSheetByName('BankStatement')).hideSheet();
  // create bank statements

  // Mark Salary Days
  var ss = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("BankStatement - " + salary_bank);
  for (var j = 2; j <= 367; j++) {
    ss.getRange("I" + j).setFormula("=IF(A"+ j +" = IFS(WEEKDAY(EOMONTH(A"+ j +",0)) = 1, EOMONTH(A"+ j +",0) - 2, WEEKDAY(EOMONTH(A"+ j +",0)) = 7, EOMONTH(A"+ j +",0) - 1, TRUE, EOMONTH(A"+ j +", 0)), 1, 0)")

    if (ss.getRange("I" + j).getValue() == 1) {
      ss.getRange("E" + j).setValue("Salary")
    }

    ss.getRange("I" + j).setFormula("")

  }
  // Mark Salary Days

  // Create Month templates
  var today = new Date();

  // Check if Jan sheet Exist
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var itt = ss.getSheetByName(mons[0]);

  for (var i = today.getMonth() + (!itt)? 0: 1; i < mons.length; i++) {
    // Copy "Template" to month sheets
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ss.setActiveSheet(ss.getSheetByName('Template'));
    ss.duplicateActiveSheet();
    ss.getActiveSheet().setName(mons[i]);
    ss.moveActiveSheet(ss.getNumSheets());

    // Set formulas for the month
    var ss = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(mons[i]);

    // Define average expenditure over the month

    ss.getRange("J4" ).setFormula("=ROUND(J3/IFS(AND(TODAY()>=A2, TODAY()<=EOMONTH(A2,0)), DAY(TODAY()), TODAY()>=EOMONTH(A2,0), DAY(EOMONTH(A2,0)), TRUE, 1), 2)");
    ss.getRange("J25").setFormula("=ROUND(J18/IFS(AND(TODAY()>=A2, TODAY()<=EOMONTH(A2,0)), DAY(EOMONTH(A2, 0)) - DAY(TODAY()) + 1, TODAY()>=EOMONTH(A2,0), DAY(EOMONTH(A2, 0)), TRUE, 1), 2)");
    ss.getRange("J26").setFormula("=ROUND(J18/IFS(AND(TODAY()>=A2, TODAY()<=EOMONTH(A2,0)), DAY(EOMONTH(A2, 0)) - DAY(TODAY() + K26), TODAY()>=EOMONTH(A2,0), DAY(EOMONTH(A2, 0)), TRUE, 1), 2)");

    // Add Bank sums for Cash & Online
    for (var j = 2; j <= 3; j++){
      var formula_withdraw = "";
      var formula_deposit = "";

      for (var k = 0; k < banks.length; k++){
        formula_withdraw = formula_withdraw + "SUMIFS('BankStatement - " + banks[k] + "'!$E:$E, 'BankStatement - " + banks[k] + "'!$B:$B, TEXT($A$2, \"MMMM\"), 'BankStatement - " + banks[k] + "'!$C:$C, $L" + j + ")" + (k < (banks.length - 1) ? " + " : "");
        formula_deposit = formula_deposit + "SUMIFS('BankStatement - " + banks[k] + "'!$F:$F, 'BankStatement - " + banks[k] + "'!$B:$B, TEXT($A$2, \"MMMM\"), 'BankStatement - " + banks[k] + "'!$C:$C, $L" + j + ")" + (k < (banks.length - 1) ? " + " : "");
      }
      ss.getRange("M" + j).setFormula("=(" + formula_withdraw + ") - (" + formula_deposit + ")");
    }
    // Add Bank sums for Cash & Online

    if( i > 0) {

      // Last Months Cash in Hand at month end
      ss.getRange("M18").setFormula("=" + mons[i-1] + "!M20");

      // number of currency notes and coins
      for (var j = 2; j < 15; j++) {
        ss.getRange("P"+j).setFormula("=" + mons[i - 1] + "!P"+j);
      }

      // Set PayTM remaining from last month
      for (var j = 2; j < 6; j++) {
        ss.getRange("T" + j).setFormula("=" + mons[i - 1] + "!N" + (j + 11));
      }

      // Set 1st date of the month
      ss.getRange("A2").setFormula("=" + mons[i-1] + "!A" + (days[i-1] + 1) + " + 1");

    } else if( i == 0 ) {
      ss.getRange("A2").setValue((i+1) + "/1/" + years);
    }

    // Clear last rows of dates from each sheet
    if ((days[i] + 2) <= 32) {
      ss.getRange("A" + (days[i] + 2) + ":H32").clearContent();
    }
  }

  // Hide template after the duplication
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ss = ss.setActiveSheet(ss.getSheetByName('Template')).hideSheet();

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ss = ss.setActiveSheet(ss.getSheetByName('BankStatement')).hideSheet();
  // Hide template after the duplication

  // Create Month templates

  // Add start date to bank, CC Statement sheet
  // Bank Sheet
  for (var i = 0; i < banks.length; i++) {
    var ss = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("BankStatement - " + banks[i]);
    ss.getRange("A2").setFormula("=" + mons[0] + "!A2")
  }
  // Bank Sheet

  // CC Statement
  var ss = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("CCStatement");
  ss.getRange("A2").setFormula("=" + mons[0] + "!A2 - 31");
  // CC Statement

  // Add start date to bank, CC Statement  sheet

};

function deleteSheets() {
  //var ss = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("BankStatement - HDFC");
  //ss.getRange("A2").setFormula("=Template!A2")

  var mons = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  var today = new Date();
  for (var i = today.getMonth()+1; i < mons.length; i++) {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ss.deleteSheet(ss.getSheetByName(mons[i]));
  }

  // UnHide template after the delete
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ss = ss.setActiveSheet(ss.getSheetByName('Template')).showSheet();
};
