module SonOfABatch
  #
  # a little helper method to help you see what fiber you're in --
  #
  #   include SonOfABatch::LogJammin
  #
  # and now you have the logline method for well-formatted justice
  #
  module LogJammin
    FIBER_IDXS = {}

    def fiber_idx
      FIBER_IDXS[Fiber.current.object_id] ||= "fiber_#{FIBER_IDXS.length}"
    end

    def logline env, run_id, indent, *segs
      env.logger.debug( [fiber_idx, object_id, run_id, " "*indent+'>', segs].flatten.join("\t") )
    end
  end


  module LoggingIterator
    def perform *args
      logline @env, @batch_id, 1, "perform", "beg"
      super
      logline @env, @batch_id, 1, "perform", "end"
    end

    def response_preamble
      logline @env, @batch_id, 2, "resp", "preamb"
      super
    end

    def handle_result req_id, req
      logline @env, @batch_id, 3, "request", "success", req_id, req.req.uri.query
      super
    end

    def handle_error req_id, req
      logline @env, @batch_id, 3, "request", "error", req_id, req.req.uri.query
      super
    end

    def response_coda
      logline @env, @batch_id, 2, "resp", "coda"
      super
    end

    def self.included base
      base.send(:include, LogJammin)
    end
  end

end
