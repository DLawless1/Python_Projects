parser = argparse.ArgumentParser(description="Download financial information.")
parser.add_argument(
    "--config",
    required=True,
    help="Configuration file")

args = parser.parse_args()

config_file_name = args.config

# Determine dates
today = datetime.datetime.today().date()
start_of_month = datetime.date(year=today.year, month=today.month, day=1)

today_str = today.isoformat()
start_date_str = (start_of_month - relativedelta(months=1)).isoformat()

hc = hero_client.HeroClient(
    config_file_name,
    scopes=['ledger_entry:read',
            'accounts_receivable.all:read'])


# Create a structure to accumulate monthly aj totals.
class TenantClinicMonthKey:
    def __init__(self, tenant_name, clinic_id, year_month):
        self._tenant_name = tenant_name
        self._clinic_id = clinic_id
        self._year_month = year_month

    def tenant_name(self):
        return self._tenant_name

    def clinic_id(self):
        return self._clinic_id

    def year_month(self):
        return self._year_month

    def __eq__(self, othr):
        return (isinstance(othr, type(self))
                and (self._tenant_name, self._clinic_id, self._year_month) ==
                (othr._tenant_name, othr._clinic_id, othr._year_month))

    def __hash__(self):
        return hash((self._tenant_name, self._clinic_id, self._year_month))


monthly_account_journal_total = {}




def accumulate_monthly_ledger_totals(tenant_name, clinic_id, posted_on_str, calculations):
    posted_on = isoparse(posted_on_str)
    key = TenantClinicMonthKey(tenant_name, clinic_id,
                               str(posted_on.year).zfill(4) + "-" + str(posted_on.month).zfill(2))

    if key not in monthly_account_journal_total:
        values = {}
        for value_key in calculations.keys():
            values[value_key] = Decimal(calculations[value_key])

        monthly_account_journal_total[key] = values
    else:
        values = monthly_account_journal_total[key]
        for value_key in calculations.keys():
            values[value_key] = Decimal(values[value_key]) + Decimal(calculations[value_key])
        monthly_account_journal_total[key] = values



with open('ledger_entries.csv', 'w') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(
        [
            'tenantName',
            'account',
            'amount',
            'patientId',
            'providerId',
            'clinicId',
            'postedOn',
            'insuranceCode',
            'description',
            'transactionId',
            'transactionType',
            'adjustmentType',
            'adjustmentCategory',
            'insurancePayer',
            'customPayer',
            'calcProduction',
            'calcAdjustments',
            'calcAdjustedProduction',
            'calcPayments',
            'calcPaymentsFromAccountCredit',
            'calcTotalPayments',
            'calcPaymentRefunds',
            'calcOverPayments',
            'calcDeposits',
            'calcCreditRefunds'
        ]
    )

    total_ledger_entries = 0
    for (tenant_name, result) in hc.download_collection(
            "/cheetah/ledger_entry?startDate=" + start_date_str + "&endDate=" + today_str):
        total_ledger_entries = total_ledger_entries + len(result)
        print('ledger_entries.csv (' + tenant_name + ') ' + str(total_ledger_entries))
        for object in result:
            accumulate_monthly_ledger_totals(tenant_name, object['clinic']['id'], object['postedOn'],
                                             object['calculations'])

            csv_writer.writerow(
                [
                    tenant_name,
                    object['account'],
                    object['amount'],
                    object['patientId'],
                    '' if object['provider'] is None else object['provider'],
                    object['clinic']['id'],
                    object['postedOn'],
                    object['insuranceCode'],
                    object['description'],
                    object['transactionId'],
                    object['transactionType'],
                    object['adjustmentType'],
                    object['adjustmentCategory'],
                    object['insurancePayer'],
                    object['customPayer'],
                    object['calculations']['production'],
                    object['calculations']['adjustments'],
                    object['calculations']['adjustedProduction'],
                    object['calculations']['payments'],
                    object['calculations']['paymentsFromAccountCredit'],
                    object['calculations']['totalPayments'],
                    object['calculations']['paymentRefunds'],
                    object['calculations']['overpayments'],
                    object['calculations']['deposits'],
                    object['calculations']['creditRefunds']
                ]
            )
ledger_entries_df = pd.read_csv('ledger_entries.csv')
print(ledger_entries_df)
with open('ledger_entry_summaries.csv', 'w') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(
        [
            'tenantName',
            'clinicId',
            'yearMonth',
            'production',
            'adjustments',
            'adjustedProduction',
            'payments',
            'paymentsFromAccountCredit',
            'totalPayments',
            'paymentRefunds',
            'overPayments',
            'deposits',
            'creditRefunds'
        ]
    )

    total_ledger_entry_summaries = 0

    for key, value in monthly_account_journal_total.items():
        csv_writer.writerow(
            [
                key.tenant_name(),
                key.clinic_id(),
                key.year_month(),
                value['production'],
                value['adjustments'],
                value['adjustedProduction'],
                value['payments'],
                value['paymentsFromAccountCredit'],
                value['totalPayments'],
                value['paymentRefunds'],
                value['overpayments'],
                value['deposits'],
                value['creditRefunds']
            ]
        )

        total_ledger_entry_summaries = total_ledger_entry_summaries + 1

print('ledger_entry_summaries.csv ' + str(total_ledger_entry_summaries))

account_receivables_summaries = {}

with open('accounts_receivable.csv', 'w') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(
        [
            'tenantName',
            'date',
            'responsiblePartyId',
            'providerId',
            'clinicId',
            'responsibleParty0to30d',
            'responsibleParty31To60',
            'responsibleParty61To90',
            'responsiblePartyOver90',
            'insuranceEstimate0to30d',
            'insuranceEstimate31To60',
            'insuranceEstimate61To90',
            'insuranceEstimateOver90d',
            'patientEstimate0to30d',
            'patientEstimate31To60',
            'patientEstimate61To90',
            'patientEstimateOver90d'
        ]
    )

    total_account_receivables = 0
    for (tenant_name, result) in hc.download_collection("/cheetah/accounts_receivable"):
        total_account_receivables = total_account_receivables + len(result)

        print('accounts_receivables.csv (' + tenant_name + ') ' + str(total_account_receivables))

        for object in result:
            csv_writer.writerow(
                [
                    tenant_name,
                    today_str,
                    object['responsibleParty']['id'],
                    object['provider']['id'],
                    object['clinic']['id'],
                    object['responsiblePartyAmount']['day0To30'],
                    object['responsiblePartyAmount']['day31To60'],
                    object['responsiblePartyAmount']['day61To90'],
                    object['responsiblePartyAmount']['dayOver90'],
                    object['insuranceEstimate']['day0To30'],
                    object['insuranceEstimate']['day31To60'],
                    object['insuranceEstimate']['day61To90'],
                    object['insuranceEstimate']['dayOver90'],
                    object['patientEstimate']['day0To30'],
                    object['patientEstimate']['day31To60'],
                    object['patientEstimate']['day61To90'],
                    object['patientEstimate']['dayOver90']
                ]
            )

            key = TenantClinicMonthKey(tenant_name, object['clinic']['id'], today_str)
            values = {
                'responsibleParty0to30d': Decimal(object['responsiblePartyAmount']['day0To30']),
                'responsiblePartyday31To60': Decimal(object['responsiblePartyAmount']['day31To60']),
                'responsiblePartyday61To90': Decimal(object['responsiblePartyAmount']['day61To90']),
                'responsiblePartydayOver90': Decimal(object['responsiblePartyAmount']['dayOver90']),
                'insuranceEstimate0to30d': Decimal(object['insuranceEstimate']['day0To30']),
                'insuranceEstimate31To60': Decimal(object['insuranceEstimate']['day31To60']),
                'insuranceEstimate61To90': Decimal(object['insuranceEstimate']['day61To90']),
                'insuranceEstimateOver90d': Decimal(object['insuranceEstimate']['dayOver90']),
                'patientEstimate0to30d': Decimal(object['patientEstimate']['day0To30']),
                'patientEstimate31To60': Decimal(object['patientEstimate']['day31To60']),
                'patientEstimate61To90': Decimal(object['patientEstimate']['day61To90']),
                'patientEstimateOver90d': Decimal(object['patientEstimate']['dayOver90'])
            }

            if key in account_receivables_summaries:
                current_summary = account_receivables_summaries[key]
                for value_key in values.keys():
                    current_summary[value_key] = current_summary[value_key] + values[value_key]
            else:
                account_receivables_summaries[key] = values

with open('accounts_receivable_summaries.csv', 'w') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(
        [
            'tenantName',
            'date',
            'clinicId',
            'responsibleParty0to30d',
            'responsiblePartyday31To60',
            'responsiblePartyday61To90',
            'responsiblePartydayOver90',
            'insuranceEstimate0to30d',
            'insuranceEstimate31To60',
            'insuranceEstimate61To90',
            'insuranceEstimateOver90d',
            'patientEstimate0to30d',
            'patientEstimate31To60',
            'patientEstimate61To90',
            'patientEstimateOver90d'
        ]
    )

    for key, value in account_receivables_summaries.items():
        csv_writer.writerow(
            [
                key.tenant_name(),
                key.year_month(),
                key.clinic_id(),
                value['responsibleParty0to30d'],
                value['responsiblePartyday31To60'],
                value['responsiblePartyday61To90'],
                value['responsiblePartydayOver90'],
                value['insuranceEstimate0to30d'],
                value['insuranceEstimate31To60'],
                value['insuranceEstimate61To90'],
                value['insuranceEstimateOver90d'],
                value['patientEstimate0to30d'],
                value['patientEstimate31To60'],
                value['patientEstimate61To90'],
                value['patientEstimateOver90d']
            ]
        )
