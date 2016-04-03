module ETL #:nodoc:
  module Execution #:nodoc:
    # Persistent class representing an ETL job
    class Job < Base
      belongs_to :batch

      def user_params
        params.require(:control_file).permit(:status, :batch_id)
      end
    end
  end
end
