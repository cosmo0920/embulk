
module Embulk
  require 'embulk/error'

  class PluginRegistry
    def initialize(category, search_prefix)
      @category = category
      @search_prefix = search_prefix
      @loaded_gems = {}
      @map = {}
    end

    attr_reader :category

    def register(type, value)
      type = type.to_sym
      @map[type] = value
    end

    def lookup(type)
      type = type.to_sym
      if value = @map[type]
        return value
      end
      if search(type)
        if value = @map[type]
          return value
        end
        raise PluginLoadError, "Unknown #{@category} plugin '#{type}'. #{@search_prefix}#{type}.rb is installed but it does not correctly register plugin."
      else
        raise PluginLoadError, "Unknown #{@category} plugin '#{type}'. #{@search_prefix}#{type}.rb is not installed. Run 'embulk gem search -rd embulk-#{@category}' command to find plugins."
      end
    end

    def search(type)
      name = "#{@search_prefix}#{type}"
      begin
        require_and_show name
        return true
      rescue LoadError => e
        # catch LoadError but don't catch ClassNotFoundException
        raise e if e.to_s =~ /java.lang.ClassNotFoundException/
        raise e if $LOAD_PATH.any? {|dir| File.exists? File.join(dir, "#{name}.rb") }
      end

      # search from $LOAD_PATH
      load_path_files = $LOAD_PATH.map do |lp|
        lpath = File.expand_path(File.join(lp, "#{name}.rb"))
        File.exist?(lpath) ? lpath : nil
      end

      paths = load_path_files.compact.sort  # sort to prefer newer version
      paths.each do |path|
        require_and_show path
        return true
      end

      # search gems
      if defined?(::Gem::Specification) && ::Gem::Specification.respond_to?(:find_all)
        specs = Gem::Specification.find_all do |spec|
          spec.contains_requirable_file? name
        end

        # prefer newer version
        specs = specs.sort_by {|spec| spec.version }
        if spec = specs.last
          spec.require_paths.each do |lib|
            require_and_show "#{spec.full_gem_path}/#{lib}/#{name}"
          end
          return true
        end
      end

      return false
    end

    def require_and_show(name)
      require name
      show_loaded_gems
    end

    def show_loaded_gems
      # TODO use logger
      Gem.loaded_specs.each do |name,spec|
        if !@loaded_gems[name] && name =~ /^embulk/
          puts "#{Time.now.strftime("%Y-%m-%d %H:%M:%S.%3N %z")}: Loaded plugin #{name} (#{spec.version})"
          @loaded_gems[name] = true
        end
      end
    end
  end
end
