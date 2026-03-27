"""
zkteco_import.py

This module handles importing attendance data from ZKTeco fingerprint machines.
The ZKTeco CSV format has columns:
- Employee ID, Date, Emp Name, IN, OUT, Work HRs, IN Place, OUT Place
"""

import io
from datetime import datetime

import pandas as pd
from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponse
from django.template.loader import render_to_string
from django.utils.translation import gettext_lazy as _

from attendance.methods.utils import strtime_seconds
from attendance.models import Attendance
from base.models import EmployeeShift
from employee.models import Employee


def parse_zkteco_time(time_val):
    """Parse time from ZKTeco format - handles both string (HH:MM:SS) and datetime objects."""
    if pd.isna(time_val) or time_val is None:
        return None
    try:
        # If it's already a time object
        if hasattr(time_val, "hour"):
            return time_val
        # If it's a datetime object (Excel might parse times as datetime)
        if isinstance(time_val, datetime):
            return time_val.time()
        # String format HH:MM:SS
        time_str = str(time_val).strip()
        if not time_str:
            return None
        return datetime.strptime(time_str, "%H:%M:%S").time()
    except (ValueError, AttributeError):
        return None


def parse_zkteco_date(date_val):
    """Parse date from ZKTeco format - handles both string (DD/MM/YYYY) and datetime objects."""
    if pd.isna(date_val) or date_val is None:
        return None
    try:
        # If it's already a date object
        if hasattr(date_val, "year") and not hasattr(date_val, "hour"):
            return date_val
        # If it's a datetime object (Excel often parses dates as datetime)
        if isinstance(date_val, datetime):
            return date_val.date()
        # If pandas Timestamp
        if hasattr(date_val, "date"):
            return date_val.date()
        # String format DD/MM/YYYY
        date_str = str(date_val).strip()
        if not date_str:
            return None
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except (ValueError, AttributeError):
        return None


def parse_work_hours(work_hrs_val):
    """Parse work hours - handles both string (HH:MM) and time objects."""
    if pd.isna(work_hrs_val) or work_hrs_val is None:
        return None
    try:
        # If it's already a time object
        if hasattr(work_hrs_val, "hour"):
            return work_hrs_val
        # If it's a datetime object
        if isinstance(work_hrs_val, datetime):
            return work_hrs_val.time()
        # String format HH:MM
        hrs_str = str(work_hrs_val).strip()
        if not hrs_str:
            return None
        return datetime.strptime(hrs_str, "%H:%M").time()
    except (ValueError, AttributeError):
        return None


def process_zkteco_data(df):
    """
    Process ZKTeco CSV data and create attendance records.

    The ZKTeco CSV has a specific format where:
    - First column contains Employee ID (only on first row for each employee)
    - Subsequent rows for same employee have empty Employee ID
    - "Sub Total" rows mark end of employee's records

    Returns:
        tuple: (created_count, error_list)
    """
    error_list = []
    attendance_list = []
    today = datetime.today().date()

    # Get all employees by badge_id for lookup
    employees_by_badge = {
        str(emp.badge_id): emp
        for emp in Employee.objects.filter(is_active=True)
        if emp.badge_id
    }

    # Get default shift (first one or None)
    default_shift = EmployeeShift.objects.first()

    # Get existing attendance records to avoid duplicates
    existing_attendance = set()
    for att in Attendance.objects.all().select_related("employee_id"):
        if att.employee_id and att.employee_id.badge_id:
            existing_attendance.add(
                (str(att.employee_id.badge_id), att.attendance_date)
            )

    current_employee_id = None
    current_employee_name = None

    for index, row in df.iterrows():
        try:
            # Skip header rows and empty rows
            if index < 5:  # First 5 rows are headers in ZKTeco format
                continue

            # Get employee ID - it's only present on first row for each employee
            emp_id_val = row.iloc[0] if not pd.isna(row.iloc[0]) else None

            # Skip "Sub Total" rows
            if emp_id_val and "Sub Total" in str(row.iloc[3]):
                continue

            # Update current employee if new ID found
            if emp_id_val and str(emp_id_val).strip():
                current_employee_id = (
                    str(int(float(emp_id_val)))
                    if isinstance(emp_id_val, (int, float))
                    else str(emp_id_val).strip()
                )

            # Get date from column 1
            date_val = row.iloc[1]
            if pd.isna(date_val) or not str(date_val).strip():
                continue

            attendance_date = parse_zkteco_date(date_val)
            if not attendance_date:
                continue

            # Get employee name from column 2
            emp_name = row.iloc[2]
            if emp_name and not pd.isna(emp_name):
                current_employee_name = str(emp_name).strip()

            # Get IN time from column 3
            in_time = parse_zkteco_time(row.iloc[3])

            # Get OUT time from column 5
            out_time = parse_zkteco_time(row.iloc[5])

            # Get Work Hours from column 6
            work_hrs = parse_work_hours(row.iloc[6])

            # Skip if no valid times
            if not in_time and not out_time:
                continue

            # Helper to append error with original row data
            def add_error(error_msg):
                error_list.append(
                    {
                        "row_data": row.tolist(),
                        "employee_id": current_employee_id,
                        "employee_name": current_employee_name,
                        "date": str(attendance_date) if attendance_date else "",
                        "error": error_msg,
                    }
                )

            # Skip if same IN and OUT time (incomplete punch)
            if in_time and out_time and in_time == out_time:
                add_error("Same IN and OUT time - incomplete punch")
                continue

            # Find employee by badge_id
            employee = employees_by_badge.get(current_employee_id)
            if not employee:
                add_error(f"Employee with Badge ID '{current_employee_id}' not found")
                continue

            # Check for duplicate
            if (current_employee_id, attendance_date) in existing_attendance:
                add_error("Attendance for this date already exists")
                continue

            # Skip future dates
            if attendance_date >= today:
                add_error("Attendance date is in the future")
                continue

            # Get employee's shift or use default
            shift = None
            if hasattr(employee, "employee_work_info") and employee.employee_work_info:
                shift = employee.employee_work_info.shift_id
            if not shift:
                shift = default_shift

            # Calculate at_work_second from work hours
            worked_hour_str = work_hrs.strftime("%H:%M") if work_hrs else "00:00"
            at_work_second = strtime_seconds(worked_hour_str) if worked_hour_str else 0

            # Create attendance record
            attendance_list.append(
                Attendance(
                    employee_id=employee,
                    shift_id=shift,
                    work_type_id=(
                        employee.employee_work_info.work_type_id
                        if hasattr(employee, "employee_work_info")
                        and employee.employee_work_info
                        else None
                    ),
                    attendance_date=attendance_date,
                    attendance_clock_in_date=attendance_date,
                    attendance_clock_in=in_time.strftime("%H:%M") if in_time else None,
                    attendance_clock_out_date=attendance_date,
                    attendance_clock_out=(
                        out_time.strftime("%H:%M") if out_time else None
                    ),
                    attendance_worked_hour=worked_hour_str,
                    at_work_second=at_work_second,
                    overtime_second=0,
                )
            )

            # Mark as existing to avoid duplicates within same import
            existing_attendance.add((current_employee_id, attendance_date))

        except Exception as e:
            error_list.append(
                {
                    "row_data": row.tolist(),
                    "employee_id": current_employee_id or "Unknown",
                    "employee_name": current_employee_name or "Unknown",
                    "date": str(row.iloc[1]) if len(row) > 1 else "Unknown",
                    "error": str(e),
                }
            )

    # Bulk create attendance records
    if attendance_list:
        Attendance.objects.bulk_create(attendance_list)

    return len(attendance_list), error_list


@login_required
@permission_required("attendance.add_attendance")
def zkteco_attendance_import(request):
    """
    Import attendance data from ZKTeco fingerprint machine CSV/Excel file.
    """
    if request.method == "POST":
        file = request.FILES.get("zkteco_import")
        if not file:
            return HttpResponse(
                "<div class='alert alert-danger'>No file uploaded</div>"
            )

        file_extension = file.name.split(".")[-1].lower()

        try:
            if file_extension == "csv":
                df = pd.read_csv(file, header=None)
            else:
                df = pd.read_excel(file, header=None)

            created_count, error_list = process_zkteco_data(df)

            # Store error list in session for download
            has_downloadable_errors = False
            if error_list:
                request.session["zkteco_import_errors"] = error_list
                has_downloadable_errors = True

            context = {
                "created_count": created_count,
                "error_count": len(error_list),
                "model": _("Attendance (ZKTeco)"),
                "errors": error_list[:20] if error_list else None,
                "has_downloadable_errors": has_downloadable_errors,
            }
            html = render_to_string(
                "attendance/zkteco/import_result.html", context, request=request
            )
            return HttpResponse(html)

        except Exception as e:
            return HttpResponse(
                f"<div class='alert alert-danger'>Error processing file: {str(e)}</div>"
            )

    return HttpResponse("<div class='alert alert-danger'>Invalid request method</div>")


@login_required
@permission_required("attendance.add_attendance")
def zkteco_download_errors(request):
    """
    Download failed import records as Excel file in original format with Error column.
    """
    error_list = request.session.get("zkteco_import_errors", [])

    if not error_list:
        return HttpResponse("No errors to download", status=404)

    # Reconstruct original spreadsheet format with Error column
    # Original columns: Employee ID, Date, Emp Name, IN, (empty), OUT, Work HRs, (empty), IN Place, OUT Place, (empty), (empty)
    rows = []

    # Add header row
    header = [
        "Employee ID",
        "Date",
        "Emp Name",
        "IN",
        "",
        "OUT",
        "Work HRs",
        "",
        "IN Place",
        "OUT Place",
        "",
        "Error",
    ]
    rows.append(header)

    for error in error_list:
        row_data = error.get("row_data", [])
        error_msg = error.get("error", "")

        # Ensure row_data has enough columns, pad if necessary
        while len(row_data) < 11:
            row_data.append("")

        # Append error message as last column
        row_data = list(row_data[:11]) + [error_msg]
        rows.append(row_data)

    # Create DataFrame
    df = pd.DataFrame(rows[1:], columns=rows[0])

    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Failed Imports")

    output.seek(0)

    response = HttpResponse(
        output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = "attachment; filename=zkteco_failed_imports.xlsx"

    # Clear session data after download
    if "zkteco_import_errors" in request.session:
        del request.session["zkteco_import_errors"]

    return response
