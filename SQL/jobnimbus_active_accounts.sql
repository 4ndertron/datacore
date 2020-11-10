select *
from jobnimbus.job as j
where j.Status in
      ('LEC - Signed Contract', 'LEC - Site Survey Scheduled', 'LEC - Cancellation Request', 'LEC - Account on Hold',
       'LEC - Site Survey Complete', 'LEC - Site Survey Verified', 'LEC - Layout Complete',
       'LEC - CAD Ready for Review', 'LEC - Engineering Requested', 'LEC - CAD Complete', 'LEC - CAD / Permit Revision',
       'LEC - CAD / Permit Revisions Complete', 'LEC - Permit Revision Complete', 'LEC - Permit Revision',
       'LEC - Permit Resubmitted', 'LEC - CAD Fixes', 'LEC - Permit Submitted', 'LEC - Ready for Permitting',
       'LEC - Waiting on MPU or Other', 'LEC - Install Started', 'LEC - Install Scheduled',
       'LEC - Waiting on Reroof/Other', 'LEC - Reschedule Install', 'LEC - Ready to Schedule',
       'LEC - Permit Rec. - Need NTP', 'LEC - Permit Received', 'LEC - Install Complete', 'LEC - Final Inspection',
       'LEC - Ready for NEM', 'LEC - Task Needed for NEM', 'LEC - NEM Submitted', 'LEC - NEM Revisions',
       'LEC - NEM Approved', 'LEC - Pending Finance Partner', 'LEC - Final Inspection Today',
       'LEC - Final Inspection Failed', 'LEC - Final Inspection Rescheduled', 'LEC - Final Inspection Passed',
       'LEC - Ready for CAD', 'LEC - CAD Requested', 'LEC - Structural Calcs Requested', 'LEC - Vivint NTP Requested',
       'LEC - READY FOR INSTALL', 'LEC - Permit Revision Install', 'LEC - Sunnova photos',
       'LEC - Installed - Need Permit', 'LEC - Installed - Layout Change', 'LEC - Ready for Final Inspection',
       'LEC - Flashing Inspection Scheduled', 'LEC - Ready to Start Install')