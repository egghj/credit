create or replace function get_loan_business(i_loan_id number)
  return table_loan
  pipelined as
  cursor c_24months_cursor(i_date date) is
    select to_char(add_months(i_date, -rownum + 1), 'yyyyMMdd') as repayment_date
      from dual
    connect by rownum <= 24;
  c_24month         c_24months_cursor%rowtype;
  c_min_overdue_day number := 3;
  c_date_format_1   varchar2(8) := 'yyyyMMdd';
  v_row             type_loan;
  temp_value        varchar2(50);
  v_count           number := 0;
  v_loan            v_loan_info%rowtype;
  v_return_date     date;
  v_sysdate         date := sysdate;
  v_current_term    number := 0;
  v_current_state   v_loan_info.LOAN_STATE%type;
  v_clear_date      date;
  v_f1              number(19, 2);
  v_f2              number(19, 2);
  v_f3              number(19, 2);
  v_f4              number(19, 2);
  v_overdue_term    number;
  v_24month_date    varchar2(8);
begin
  v_row := type_loan(null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null);
  if i_loan_id is not null then
    select count(1) into v_count from v_loan_info t where t.id = i_loan_id;
    if v_count > 0 then
      select * into v_loan from v_loan_info t where t.id = i_loan_id;
    
      --当月应还款日
      if to_number(to_char(v_sysdate, 'dd')) > v_loan.promise_return_date then
        v_return_date := to_date(to_char(v_sysdate, 'yyyyMM') ||
                                 v_loan.promise_return_date,
                                 'yyyyMMdd');
      else
        v_return_date := to_date(to_char(add_months(v_sysdate, -1),
                                         'yyyyMM') ||
                                 v_loan.promise_return_date,
                                 'yyyyMMdd');
      end if;
    
      --求当前期数 ,过了结清日期的借款当前期为最后一期
      if v_loan.endrdate > v_return_date then
        if v_loan.startrdate > v_return_date then
          v_current_term := 0; --刚开户
        else
          v_current_term := round(months_between(v_return_date,
                                                 v_loan.startrdate)) + 1;
        end if;
      else
        v_current_term := v_loan.time;
      end if;
    
      --当前状态
      if v_current_term < 1 then
        v_current_state := '正常';
      else
        select count(1)
          into v_count
          from loan_repayment_detail d
         where d.loan_id = i_loan_id
           and d.current_term = v_loan.time
           and d.fact_return_date - c_min_overdue_day <= v_return_date;
        if v_loan.loan_state = '结清' and v_count > 0 then
          v_current_state := '结清';
        else
          select count(1)
            into v_count
            from loan_repayment_detail d
           where d.loan_id = i_loan_id
             and d.current_term = v_current_term
             and d.fact_return_date - c_min_overdue_day <= v_return_date;
          if v_count > 0 then
            v_current_state := '正常';
          else
            v_current_state := '逾期';
          end if;
        end if;
      end if;
    
      --结清日期
      if v_current_state = '结清' then
        select d.fact_return_date
          into v_clear_date
          from loan_repayment_detail d
         where d.loan_id = i_loan_id
           and d.current_term = v_loan.time;
      end if;
    
      --结算、应还款日期 YYYYMMDD
      temp_value := '';
      if v_current_term < 1 then
        temp_value := to_char(v_loan.grant_money_date, c_date_format_1); --放款日期
      else
        if v_current_state = '结清' then
          if to_number(to_char(v_clear_date, 'dd')) >
             v_loan.promise_return_date then
            v_return_date := add_months(to_date(to_char(v_clear_date,
                                                        'yyyyMM') ||
                                                v_loan.promise_return_date,
                                                'yyyyMMdd'),
                                        1);
          else
            v_return_date := to_date(to_char(v_clear_date, 'yyyyMM') ||
                                     v_loan.promise_return_date,
                                     'yyyyMMdd');
          end if;
          temp_value := to_char(v_return_date, c_date_format_1);
        else
          temp_value := to_char(v_return_date, c_date_format_1);
        end if;
      end if;
      v_row.loan_id        := i_loan_id;
      v_row.repayment_date := temp_value;
    
      --最近一次实际还款 YYYYMMDD
      temp_value := '';
      select case
               when max(d.fact_return_date) is null then
                to_char(v_loan.grant_money_date, c_date_format_1)
               else
                to_char(max(d.fact_return_date), c_date_format_1)
             end
        into temp_value
        from loan_repayment_detail d
       where d.loan_id = i_loan_id
      /*and fact_return_date is not null*/
      ;
      v_row.lately_real_repay_date := temp_value;
    
      --本月应还款金额
      temp_value := '';
      if v_current_term = 0 then
        --新开户
        temp_value := '0';
      elsif v_current_state = '结清' then
        --刚好当前月结清
        select to_char(round(d.returneterm))
          into temp_value
          from loan_repayment_detail d
         where d.loan_id = i_loan_id
           and d.current_term = v_current_term;
      else
        select to_char(round(sum(d.returneterm)))
          into temp_value
          from loan_repayment_detail d
         where d.loan_id = i_loan_id
           and (d.fact_return_date is null or exists
                (select fact_return_date
                   from loan_repayment_detail r
                  where r.loan_id = i_loan_id
                    and current_term = v_current_term
                    and d.fact_return_date = r.fact_return_date));
      end if;
      if temp_value is null then
        temp_value := '0';
      end if;
      v_row.cur_month_should_amount := temp_value;
    
      --本月实际还款金额
      temp_value := '';
      select to_char(round(sum(amount)))
        into temp_value
        from offer_repay_info
       where loan_id = i_loan_id
         and trade_date between add_months(v_loan.startrdate, 1) and
             add_months(v_loan.startrdate, v_current_term)
         and trade_code not in
             ('1003', '1005', '4001', '4002', '5002', '5004', '5003');
      if temp_value is null then
        temp_value := '0';
      end if;
      v_row.cur_month_real_repay_amount := temp_value;
    
      v_row.balance := to_char(round(v_loan.residual_pact_money));
    
      --当前逾期期数
      temp_value := '';
      if v_current_state = '逾期' then
        select to_char(count(*))
          into temp_value
          from loan_repayment_detail d
         where d.loan_id = i_loan_id
           and return_date <= v_return_date
           and (d.fact_return_date is null or exists
                (select 1
                   from loan_repayment_detail r
                  where r.loan_id = i_loan_id
                    and current_term = v_current_term
                    and d.fact_return_date = r.fact_return_date));
      else
        temp_value := '0';
      end if;
      if temp_value is null then
        temp_value := '0';
      end if;
      v_row.current_overdue_period := temp_value;
    
      --当前逾期总额
      temp_value := '';
      if v_current_state = '逾期' then
        begin
          select to_char(round(sum(d.returneterm)))
            into temp_value
            from loan_repayment_detail d
           where d.loan_id = i_loan_id
             and d.current_term <= v_current_term
             and (d.fact_return_date is null or exists
                  (select 1
                     from loan_repayment_detail r
                    where r.loan_id = i_loan_id
                      and current_term = v_current_term
                      and d.fact_return_date = r.fact_return_date));
        
        end;
      else
        temp_value := '0';
      end if;
      if temp_value is null then
        temp_value := '0';
      end if;
      v_row.current_overdue_total_amount := temp_value;
    
      --30-180逾期金额
      select sum(case
                   when (v_return_date - d.return_date) BETWEEN 31 and 60 THEN
                    (case
                      when (d.returneterm - d.current_accrual) > d.deficit THEN
                       d.deficit
                      else
                       (d.returneterm - d.current_accrual)
                    end)
                   else
                    0
                 end) as f1,
             sum(case
                   when (v_return_date - d.return_date) BETWEEN 61 and 90 THEN
                    (case
                      when (d.returneterm - d.current_accrual) > d.deficit THEN
                       d.deficit
                      else
                       (d.returneterm - d.current_accrual)
                    end)
                   else
                    0
                 end) as f2,
             sum(case
                   when (v_return_date - d.return_date) BETWEEN 91 and 180 THEN
                    (case
                      when (d.returneterm - d.current_accrual) > d.deficit THEN
                       d.deficit
                      else
                       (d.returneterm - d.current_accrual)
                    end)
                   else
                    0
                 end) as f3,
             sum(case
                   when (v_return_date - d.return_date) > 180 THEN
                    (case
                      when (d.returneterm - d.current_accrual) > d.deficit THEN
                       d.deficit
                      else
                       (d.returneterm - d.current_accrual)
                    end)
                   else
                    0
                 end) as f4
        into v_f1, v_f2, v_f3, v_f4
        from loan_repayment_detail d
       where d.loan_id = i_loan_id
         and d.current_term <= v_current_term
         and (d.fact_return_date is null or exists
              (select 1
                 from loan_repayment_detail r
                where r.loan_id = i_loan_id
                  and r.current_term = v_current_term
                  and d.fact_return_date = r.fact_return_date))
       group by d.loan_id;
    
      v_row.overdue_loan_principal31_60 := to_char(round(case
                                                           when v_f1 is null then
                                                            0
                                                           else
                                                            v_f1
                                                         end));
      v_row.overdue_loan_principal61_90 := to_char(round(case
                                                           when v_f2 is null then
                                                            0
                                                           else
                                                            v_f2
                                                         end));
      v_row.overdue_loan_principal91_180 := to_char(round(case
                                                            when v_f3 is null then
                                                             0
                                                            else
                                                             v_f3
                                                          end));
      v_row.overdue_loan_principal180 := to_char(round(case
                                                         when v_f4 is null then
                                                          0
                                                         else
                                                          v_f4
                                                       end));
    
      --累计逾期期数
      select count(1)
        into v_overdue_term
        from loan_repayment_detail t
       where t.loan_id = i_loan_id
         and t.current_term <= v_current_term
         and ((t.fact_return_date is not null and
             t.fact_return_date - c_min_overdue_day > t.return_date) or
             t.fact_return_date is null);
      v_row.total_overdue_period := to_char(v_overdue_term);
    
      --历史最高逾期期数
      select max(overdue_term)
        into v_overdue_term
        from (select fact_return_date, sum(overdue_term) as overdue_term
                from (select case
                               when fact_return_date is null then
                                case
                                  when current_date > return_date then
                                   1
                                  else
                                   0
                                end
                               else
                                case
                                  when fact_return_date - c_min_overdue_day >
                                       return_date then
                                   1
                                  else
                                   0
                                end
                             end as overdue_term,
                             case
                               when fact_return_date is null then
                                v_return_date
                               else
                                fact_return_date
                             end as fact_return_date
                        from loan_repayment_detail
                       where loan_id = i_loan_id
                         and current_term <= v_current_term) a
               group by fact_return_date) aa;
      v_row.highest_overdue_period := to_char(v_overdue_term);
    
      for c_24month in c_24months_cursor(v_return_date) loop
        v_24month_date := c_24month.repayment_date;
      end loop;
    
      --协定期还款额
      temp_value := '';
      select to_char(round(sum(returneterm)))
        into temp_value
        from loan_repayment_detail d
       where d.loan_id = i_loan_Id
         and d.current_term = decode(v_current_term, 0, 1, v_current_term);
      v_row.treaty_repayment_amount := temp_value;
    
      --剩余还款月数
      temp_value := '';
      if v_current_term < 1 then
        temp_value := to_char(v_loan.time);
      else
        if v_current_state = '结清' then
          temp_value := '0';
        else
          select to_char(count(1))
            into temp_value
            from loan_repayment_detail d
           where d.loan_id = i_loan_id
             and d.current_term >= v_current_term
             and d.fact_return_date is null;
        end if;
      end if;
      v_row.remainder_repayment_months := temp_value;
    
      --账户状态
      temp_value := '';
      if v_current_state = '正常' then
        temp_value := '1';
      elsif v_current_state = '逾期' then
        temp_value := '2';
      elsif v_current_state = '结清' then
        temp_value := '3';
      end if;
      v_row.account_state := temp_value;
    
      pipe row(v_row);
    end if;
  end if;
  return;
end get_loan_business;
/
