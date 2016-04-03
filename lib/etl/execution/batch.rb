module ETL #:nodoc:
  module Execution #:nodoc:
    # Persistent class representing an ETL batch
    class Batch < Base
      belongs_to :batch
      has_many :batches
      has_many :jobs

      def user_params
        params.require(:batch_file).permit(:status, :completed_at)
      end
    end
  end
end
