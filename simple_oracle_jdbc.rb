module SimpleOracleJDBC

  def each_row(rs)
    begin
      while(r = rs.next)
        yield row_as_hash(rs)
      end
    ensure
      close_result_set(rs)
      @sw.close
    end
  end

  def db_query(conn, query)
    @sw = conn.create_statement
    @sw.execute_query query
  end

  def db_upsert(conn, query)
    @sw = conn.create_statement
    @sw.execute query
  end

  def first_row(rs)
    if rs.next
      row_as_hash(rs)
    else
      {}
    end
  end

  def row_as_hash(result_set)
    mdata = result_set.get_meta_data
    cols = mdata.get_column_count
    h = Hash.new
    1.upto(cols) do |i|
      h[mdata.get_column_name(i)] = retrieve_value(result_set, i)
    end
    h
  end

  def close_result_set(rs)
    if rs
      rs.close
      rs = nil
    end
  end

  def retrieve_date(obj, i)
    jdate = obj.get_date(i)
    java_date_as_date(jdate)
  end

  def retrieve_time(obj, i)
    jdate = obj.get_timestamp(i)
    java_date_as_time(jdate)
  end

  def retrieve_string(obj, i)
    obj.get_string(i)
  end

  def retrieve_int(obj, i)
    v = obj.get_long(i)
    if obj.was_null
      v = nil
    end
    java_integer_as_integer(v)
  end

  def retrieve_number(obj, i)
    v = obj.get_number(i)
    java_number_as_float(v)
  end

  def retrieve_refcursor(obj, i)
    rset = obj.get_object(i)
    # Dummy connection passed as it is never needed?
    results = Sql.new(nil)
    results.result_set = rset
    results
  end

  def retrieve_raw(obj, i)
    v = obj.get_raw(i)
    oracle_raw_as_string(v)
  end


  def retrieve_value(obj, i)
    retrieve_string(obj, i)
  end

  def java_date_as_date(v)
    if v
      Date.new(v.get_year+1900, v.get_month+1, v.get_date)
    else
      nil
    end
  end

  def java_date_as_time(v)
    if v
      Time.at(v.get_time.to_f / 1000)
    else
      nil
    end
  end

  def java_number_as_float(v)
    if v
      v.double_value
    else
      nil
    end
  end

  def java_integer_as_integer(v)
    # JRuby automatically converts INT to INT
    v
  end

  def java_string_as_string(v)
    # JRubyt automatically converts to a Ruby string
    v
  end

  def oracle_raw_as_string(v)
    if v
      v.string_value
    else
      nil
    end
  end

  def ruby_date_as_jdbc_date(v)
    if v
      jdbc_date = Java::JavaSql::Date.new(v.strftime("%s").to_f * 1000)
    else
      nil
    end
  end

  def ruby_time_as_jdbc_timestamp(v)
    if v
      TIMESTAMP.new(Java::JavaSql::Timestamp.new(v.to_f * 1000))
    else
      nil
    end
  end

  def ruby_any_date_as_jdbc_date(v)
    if v
      if v.is_a? Date
        ruby_date_as_jdbc_date(v)
      elsif v.is_a? Time
        ruby_time_as_jdbc_timestamp(v)
      else
        raise "#{v.class}: unimplemented Ruby date type for arrays. Use Date or Time"
      end
    else
      nil
    end
  end

  def ruby_number_as_jdbc_number(v)
    if v
      # Avoid warning that appeared in JRuby 1.7.3. There are many signatures of
      # Java::OracleSql::NUMBER and it has to pick one. This causes a warning. This
      # technique works around the warning and forces it to the the signiture with a
      # double input - see https://github.com/jruby/jruby/wiki/CallingJavaFromJRuby
      # under the Constructors section.
      construct = Java::OracleSql::NUMBER.java_class.constructor(Java::double)
      construct.new_instance(v)
    else
      nil
    end
  end

  def ruby_raw_string_as_jdbc_raw(v)
    if v
      Java::OracleSql::RAW.new(v)
    else
      v
    end
  end

end

