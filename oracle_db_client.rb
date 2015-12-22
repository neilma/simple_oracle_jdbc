require File.expand_path('../simple_oracle_jdbc', __FILE__)

class OracleDbClient
  include SimpleOracleJDBC
  include ApplicationHelper
  attr_accessor :db_config, :username, :password, :db_env

  def initialize(username=nil, password=nil, db_env=:virtual)
    @db_env = db_env || :virtual
    @db_config = load_config
    @username = username || db_config[:username]
    @password = password || db_config[:password]
  end

  def conn
    Java::JavaClass.for_name(db_config[:driver])
    begin
      conn = java.sql.DriverManager.getConnection("#{db_config[:conn_path]}#{db_config[:database]}",
                                                  username, password)
    rescue => e
      return {error: humanize_pms_error_msg(e.message)}
    end
    yield(conn)
  end

  def query(statement, transaction_type='db_query')
    result_set = nil
    begin
      rval = conn { |cursor| result_set = public_send(transaction_type, cursor, statement) }
    rescue => e
      return e
    end
    return rval if rval.is_a?(Hash)
    transaction_type == 'db_query' ? to_hash(result_set) : true
  end

  def to_hash(result_set)
    [].tap { |hash_result| each_row(result_set) { |row| hash_result << row } }
  end

  private
  def load_config
    HashWithIndifferentAccess.new(YAML.load(File.read("#{Rails.root}/config/client_db.yml")))[db_env.downcase]
  end

  def humanize_pms_error_msg(msg)
    if msg.include?('invalid database address')
      msg + ' or incorrect credentials.'
    else
      msg
    end
  end

end
