require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "SonOfABatch" do

  describe "apikey" do
    it "requires an API key"
  end

  describe "timeout param" do
    it "accepts a timeout param"
    it "coerces the timeout param"
  end

  describe "request format" do
    it "requires well-formed URLs (BadRequestError)"
    it "requires one or more URLs (BadRequestError)"
    it "accepts at most 100 URLs at a time (BadRequestError)"

    it "raises ForbiddenError if URL is not whitelisted"
  end

  describe "response" do
    it "returns a well-formed hash" do
      # parsed_response = {}
      # parsed_response['results'].should be_a_kind_of(Hash)
      # parsed_response['errors'].should be_a_kind_of(Hash)
    end

    it "sends back the errors last" do
    end

    it "has the right commas when there are no good results"
    it "has the right commas when there is one good result"
    it "has the right commas when there are many good results"

    it "escapes the target response"
    it "has no newlines or tabs even when the target response does"
  end

  describe "proxy" do
    it "sends requests to multiple downstream hosts"
  end

end
